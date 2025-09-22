from faker import Faker
from typing import Dict, Any, List
import logging
import pyodbc
from datetime import datetime
import os
import hashlib  # Add hashlib for consistent hashing
from models import Workflow, WorkflowExecution, WorkflowStatus, ColumnMapping
from services.database_service import DatabaseService
from services.workflow_service import WorkflowService
import asyncio

logger = logging.getLogger(__name__)

# Development mode flag
DEV_MODE = os.getenv('DEVELOPMENT_MODE', 'False').lower() == 'true'

def hash_seed(text):
    """Generate a consistent integer seed from input text"""
    if not text or not isinstance(text, str):
        # Handle None or non-string values
        text = str(text) if text is not None else ""
    return int(hashlib.sha256(text.encode('utf-8')).hexdigest(), 16) % (10 ** 8)

def get_deterministic_faker(seed_value):
    """Create a Faker instance with a specific seed"""
    fake = Faker()
    fake.seed_instance(seed_value)
    return fake

class DataMaskingService:
    """Service for masking PII data using Faker library"""

    def __init__(self, database_service: DatabaseService, workflow_service: WorkflowService):
        self.database_service = database_service
        self.workflow_service = workflow_service
        self.faker = Faker()

        # Mapping of PII attributes to Faker methods with deterministic approach
        # Each lambda accepts the original value to generate consistent results
        self.pii_mapping = {
            'address': lambda original_value: get_deterministic_faker(hash_seed(original_value)).address(),
            'city': lambda original_value: get_deterministic_faker(hash_seed(original_value)).city(),
            'city_prefix': lambda original_value: get_deterministic_faker(hash_seed(original_value)).city_prefix(),
            'city_suffix': lambda original_value: get_deterministic_faker(hash_seed(original_value)).city_suffix(),
            'company': lambda original_value: get_deterministic_faker(hash_seed(original_value)).company(),
            'company_email': lambda original_value: get_deterministic_faker(hash_seed(original_value)).company_email(),
            'company_suffix': lambda original_value: get_deterministic_faker(hash_seed(original_value)).company_suffix(),
            'country': lambda original_value: get_deterministic_faker(hash_seed(original_value)).country(),
            'country_calling_code': lambda original_value: get_deterministic_faker(hash_seed(original_value)).country_calling_code(),
            'country_code': lambda original_value: get_deterministic_faker(hash_seed(original_value)).country_code(),
            'date_of_birth': lambda original_value: get_deterministic_faker(hash_seed(original_value)).date_of_birth(),
            'email': lambda original_value: get_deterministic_faker(hash_seed(original_value)).email(),
            'first_name': lambda original_value: get_deterministic_faker(hash_seed(original_value)).first_name(),
            'job': lambda original_value: get_deterministic_faker(hash_seed(original_value)).job(),
            'last_name': lambda original_value: get_deterministic_faker(hash_seed(original_value)).last_name(),
            'name': lambda original_value: get_deterministic_faker(hash_seed(original_value)).name(),
            'passport_dob': lambda original_value: get_deterministic_faker(hash_seed(original_value)).passport_dob(),
            'passport_full': lambda original_value: get_deterministic_faker(hash_seed(original_value)).passport_full(),
            'passport_gender': lambda original_value: get_deterministic_faker(hash_seed(original_value)).passport_gender(),
            'passport_number': lambda original_value: get_deterministic_faker(hash_seed(original_value)).passport_number(),
            'passport_owner': lambda original_value: get_deterministic_faker(hash_seed(original_value)).passport_owner(),
            'phone_number': lambda original_value: get_deterministic_faker(hash_seed(original_value)).phone_number(),
            'postalcode': lambda original_value: get_deterministic_faker(hash_seed(original_value)).postcode(),
            'postcode': lambda original_value: get_deterministic_faker(hash_seed(original_value)).postcode(),
            'profile': lambda original_value: get_deterministic_faker(hash_seed(original_value)).profile(),
            'secondary_address': lambda original_value: get_deterministic_faker(hash_seed(original_value)).secondary_address(),
            'simple_profile': lambda original_value: get_deterministic_faker(hash_seed(original_value)).simple_profile(),
            'ssn': lambda original_value: get_deterministic_faker(hash_seed(original_value)).ssn(),
            'state': lambda original_value: get_deterministic_faker(hash_seed(original_value)).state(),
            'state_abbr': lambda original_value: get_deterministic_faker(hash_seed(original_value)).state_abbr(),
            'street_address': lambda original_value: get_deterministic_faker(hash_seed(original_value)).street_address(),
            'street_name': lambda original_value: get_deterministic_faker(hash_seed(original_value)).street_name(),
            'street_suffix': lambda original_value: get_deterministic_faker(hash_seed(original_value)).street_suffix(),
            'zipcode': lambda original_value: get_deterministic_faker(hash_seed(original_value)).zipcode(),
            'zipcode_in_state': lambda original_value: get_deterministic_faker(hash_seed(original_value)).zipcode_in_state(),
            'zipcode_plus4': lambda original_value: get_deterministic_faker(hash_seed(original_value)).zipcode_plus4(),
        }

    async def execute_workflow(self, workflow_id: str) -> WorkflowExecution:
        """Execute a masking workflow"""
        # Create execution record
        execution = await self.workflow_service.create_execution(workflow_id)

        try:
            # Get workflow details
            workflow = await self.workflow_service.get_workflow_by_id(workflow_id)
            if not workflow:
                raise ValueError(f"Workflow {workflow_id} not found")

            # Update workflow status to running
            await self.workflow_service.update_workflow_status(workflow_id, WorkflowStatus.RUNNING)

            # Get database connections
            source_conn = await self.database_service.get_connection_by_id(workflow.source_connection_id)
            dest_conn = await self.database_service.get_connection_by_id(workflow.destination_connection_id)

            if not source_conn or not dest_conn:
                raise ValueError("Source or destination connection not found")

            # Get passwords from Key Vault
            source_password = await self.database_service.get_password_from_keyvault(
                source_conn.password_key_vault_name
            )
            dest_password = await self.database_service.get_password_from_keyvault(
                dest_conn.password_key_vault_name
            )

            # Build connection strings
            source_conn_str = self.database_service._build_azure_sql_connection_string(
                source_conn, source_password
            )
            dest_conn_str = self.database_service._build_azure_sql_connection_string(
                dest_conn, dest_password
            )

            total_records = 0

            # Process each table mapping
            for table_mapping in workflow.table_mappings:
                records_processed = await self._process_table_mapping(
                    source_conn_str, dest_conn_str, table_mapping, execution
                )
                total_records += records_processed

                # Update execution with progress
                execution.execution_logs.append(
                    f"Processed table {table_mapping.source_table}: {records_processed} records"
                )
                await self.workflow_service.update_execution(execution)

            # Mark execution as completed
            execution.status = WorkflowStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            execution.records_processed = total_records
            execution.execution_logs.append(f"Workflow completed successfully. Total records: {total_records}")

            # Update workflow status
            await self.workflow_service.update_workflow_status(workflow_id, WorkflowStatus.COMPLETED)

        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            execution.status = WorkflowStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error_message = str(e)
            execution.execution_logs.append(f"Workflow failed: {str(e)}")

            # Update workflow status
            await self.workflow_service.update_workflow_status(workflow_id, WorkflowStatus.FAILED)

        finally:
            # Save final execution state
            await self.workflow_service.update_execution(execution)

        return execution


    async def _process_table_mapping(self, source_conn_str: str, dest_conn_str: str,
                                   table_mapping, execution: WorkflowExecution) -> int:
        """Process a single table mapping"""
        try:
            # Step 1: Table Management - Clear existing data from destination table
            # Note: Destination table is expected to be created beforehand by DB team
            logger.info(f"Clearing existing data from destination table: {table_mapping.destination_table}")
            execution.execution_logs.append(f"Clearing existing data from destination table: {table_mapping.destination_table}")

            await self._clear_destination_table(dest_conn_str, table_mapping.destination_table)

            execution.execution_logs.append(f"Successfully cleared destination table: {table_mapping.destination_table}")

            # Step 2: Data Processing - Get PII columns that need masking
            pii_columns = [col for col in table_mapping.column_mappings if col.is_pii]

            # Build column lists for SELECT and INSERT
            source_columns = [col.source_column for col in table_mapping.column_mappings]
            dest_columns = [col.destination_column for col in table_mapping.column_mappings]

            # Read data from source
            with pyodbc.connect(source_conn_str, timeout=60) as source_conn:
                cursor = source_conn.cursor()

                # Build SELECT query
                select_query = f"SELECT {', '.join(source_columns)} FROM {table_mapping.source_table}"
                cursor.execute(select_query)

                # Fetch data in batches
                batch_size = 1000
                records_processed = 0

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    # Process and mask data
                    masked_rows = []
                    for row in rows:
                        masked_row = list(row)
                        # Apply masking to PII columns
                        for i, col_mapping in enumerate(table_mapping.column_mappings):
                            if col_mapping.is_pii and col_mapping.pii_attribute:
                                if col_mapping.pii_attribute in self.pii_mapping:
                                    try:
                                        # Skip masking if the value is blank or empty
                                        if masked_row[i] is None or str(masked_row[i]).strip() == "":
                                            continue
                                        # Use the original value to ensure deterministic masking
                                        original_value = str(masked_row[i])
                                        masked_value = self.pii_mapping[col_mapping.pii_attribute](original_value)
                                        # Handle different data types
                                        if isinstance(masked_value, dict):
                                            masked_value = str(masked_value)
                                        masked_row[i] = masked_value
                                    except Exception as e:
                                        logger.warning(f"Failed to mask column {col_mapping.source_column}: {e}")

                        masked_rows.append(masked_row)

                    # Insert masked data into destination
                    await self._insert_masked_data(
                        dest_conn_str, table_mapping.destination_table,
                        dest_columns, masked_rows
                    )

                    records_processed += len(masked_rows)

                    # Log progress
                    execution.execution_logs.append(
                        f"Processed batch for {table_mapping.source_table}: {records_processed} records so far"
                    )

                    # Update execution periodically
                    if records_processed % 5000 == 0:
                        execution.records_processed = records_processed
                        await self.workflow_service.update_execution(execution)

            return records_processed

        except Exception as e:
            logger.error(f"Failed to process table mapping {table_mapping.source_table}: {e}")
            raise

    async def _clear_destination_table(self, dest_conn_str: str, table_name: str):
        """Clear all data from destination table"""
        try:
            with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
                cursor = dest_conn.cursor()

                # Delete all records from the table
                delete_query = f"DELETE FROM [{table_name}]"

                logger.info(f"Clearing all data from destination table: {table_name}")
                cursor.execute(delete_query)
                dest_conn.commit()

                # Log the number of affected rows
                logger.info(f"Cleared {cursor.rowcount} rows from table {table_name}")

        except Exception as e:
            logger.error(f"Failed to clear destination table {table_name}: {e}")
            raise


    async def _insert_masked_data(self, dest_conn_str: str, table_name: str,
                                columns: List[str], data: List[List[Any]]):
        """Insert masked data into destination table"""
        try:
            with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
                cursor = dest_conn.cursor()

                # Build INSERT query
                placeholders = ', '.join(['?' for _ in columns])
                insert_query = f"INSERT INTO [{table_name}] ([{'], ['.join(columns)}]) VALUES ({placeholders})"

                # Execute batch insert
                cursor.executemany(insert_query, data)
                dest_conn.commit()

        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
            raise
    def generate_sample_masked_data(self, pii_attribute: str, count: int = 5, sample_value: str = "sample") -> List[str]:
        """Generate sample masked data for preview"""
        if pii_attribute not in self.pii_mapping:
            return [f"Unknown attribute: {pii_attribute}"] * count

        try:
            samples = []
            # Generate deterministic samples to show consistency
            for i in range(count):
                # Use incremental sample value to show different results
                test_value = f"{sample_value}_{i}"
                value = self.pii_mapping[pii_attribute](test_value)
                if isinstance(value, dict):
                    value = str(value)
                samples.append(str(value))

            # Development mode: log sample data to console
            if DEV_MODE:
                logger.info(f"Generated sample data for {pii_attribute}: {samples}")

            return samples
        except Exception as e:
            logger.error(f"Failed to generate sample data for {pii_attribute}: {e}")
            return [f"Error generating sample: {str(e)}"] * count
 