from faker import Faker
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import hashlib
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

# Try to import pyodbc, but make it optional
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    logging.warning("pyodbc not installed. SQL Server connections will not be available.")

from ..models.workflow import Workflow, WorkflowExecution, WorkflowStatus
from ..models.mapping import ColumnMapping
from ..crud.connection import decrypt_password
from ..crud.workflow import (
    get_workflow,
    update_workflow_execution,
    create_workflow_execution
)
from ..crud.connection import get_connection

logger = logging.getLogger(__name__)


def hash_seed(text):
    """Generate a consistent integer seed from input text"""
    if not text or not isinstance(text, str):
        text = str(text) if text is not None else ""
    return int(hashlib.sha256(text.encode('utf-8')).hexdigest(), 16) % (10 ** 8)


def get_deterministic_faker(seed_value):
    """Create a Faker instance with a specific seed for deterministic output"""
    fake = Faker()
    fake.seed_instance(seed_value)
    return fake


class DataMaskingService:
    """Service for masking PII data using Faker library"""

    def __init__(self):
        self.faker = Faker()

        # Mapping of PII attributes to Faker methods with deterministic approach
        self.pii_mapping = {
            'address': lambda val: get_deterministic_faker(hash_seed(val)).address(),
            'city': lambda val: get_deterministic_faker(hash_seed(val)).city(),
            'city_prefix': lambda val: get_deterministic_faker(hash_seed(val)).city_prefix(),
            'city_suffix': lambda val: get_deterministic_faker(hash_seed(val)).city_suffix(),
            'company': lambda val: get_deterministic_faker(hash_seed(val)).company(),
            'company_email': lambda val: get_deterministic_faker(hash_seed(val)).company_email(),
            'company_suffix': lambda val: get_deterministic_faker(hash_seed(val)).company_suffix(),
            'country': lambda val: get_deterministic_faker(hash_seed(val)).country(),
            'country_calling_code': lambda val: get_deterministic_faker(hash_seed(val)).country_calling_code(),
            'country_code': lambda val: get_deterministic_faker(hash_seed(val)).country_code(),
            'date_of_birth': lambda val: str(get_deterministic_faker(hash_seed(val)).date_of_birth()),
            'email': lambda val: get_deterministic_faker(hash_seed(val)).email(),
            'first_name': lambda val: get_deterministic_faker(hash_seed(val)).first_name(),
            'job': lambda val: get_deterministic_faker(hash_seed(val)).job(),
            'last_name': lambda val: get_deterministic_faker(hash_seed(val)).last_name(),
            'name': lambda val: get_deterministic_faker(hash_seed(val)).name(),
            'passport_dob': lambda val: str(get_deterministic_faker(hash_seed(val)).passport_dob()),
            'passport_full': lambda val: str(get_deterministic_faker(hash_seed(val)).passport_full()),
            'passport_gender': lambda val: get_deterministic_faker(hash_seed(val)).passport_gender(),
            'passport_number': lambda val: get_deterministic_faker(hash_seed(val)).passport_number(),
            'passport_owner': lambda val: str(get_deterministic_faker(hash_seed(val)).passport_owner()),
            'phone_number': lambda val: get_deterministic_faker(hash_seed(val)).phone_number(),
            'postalcode': lambda val: get_deterministic_faker(hash_seed(val)).postcode(),
            'postcode': lambda val: get_deterministic_faker(hash_seed(val)).postcode(),
            'profile': lambda val: str(get_deterministic_faker(hash_seed(val)).profile()),
            'secondary_address': lambda val: get_deterministic_faker(hash_seed(val)).secondary_address(),
            'simple_profile': lambda val: str(get_deterministic_faker(hash_seed(val)).simple_profile()),
            'ssn': lambda val: get_deterministic_faker(hash_seed(val)).ssn(),
            'state': lambda val: get_deterministic_faker(hash_seed(val)).state(),
            'state_abbr': lambda val: get_deterministic_faker(hash_seed(val)).state_abbr(),
            'street_address': lambda val: get_deterministic_faker(hash_seed(val)).street_address(),
            'street_name': lambda val: get_deterministic_faker(hash_seed(val)).street_name(),
            'street_suffix': lambda val: get_deterministic_faker(hash_seed(val)).street_suffix(),
            'zipcode': lambda val: get_deterministic_faker(hash_seed(val)).zipcode(),
            'zipcode_in_state': lambda val: get_deterministic_faker(hash_seed(val)).zipcode_in_state(),
            'zipcode_plus4': lambda val: get_deterministic_faker(hash_seed(val)).zipcode_plus4(),
        }

    async def execute_workflow(
        self,
        db: AsyncSession,
        workflow_id: int,
        user_id: int
    ) -> WorkflowExecution:
        """Execute a masking workflow"""
        # Check if pyodbc is available
        if not PYODBC_AVAILABLE:
            raise RuntimeError(
                "pyodbc is not installed. Please install Microsoft C++ Build Tools and then install pyodbc. "
                "See: https://visualstudio.microsoft.com/visual-cpp-build-tools/"
            )

        # Create execution record
        execution = await create_workflow_execution(db, workflow_id, user_id)
        execution_logs = []

        try:
            # Get workflow details
            workflow = await get_workflow(db, workflow_id)
            if not workflow:
                raise ValueError(f"Workflow {workflow_id} not found")

            # Check workflow ownership or admin access
            if workflow.user_id != user_id:
                # TODO: Add admin check here
                raise ValueError("Unauthorized to execute this workflow")

            # Get database connections
            source_conn = await get_connection(db, workflow.source_connection_id)
            dest_conn = await get_connection(db, workflow.destination_connection_id)

            if not source_conn or not dest_conn:
                raise ValueError("Source or destination connection not found")

            # Decrypt passwords
            source_password = decrypt_password(source_conn.password_encrypted)
            dest_password = decrypt_password(dest_conn.password_encrypted)

            # Build connection strings
            source_conn_str = self._build_connection_string(
                source_conn.connection_type,
                source_conn.server,
                source_conn.database,
                source_conn.username,
                source_password,
                source_conn.port
            )
            dest_conn_str = self._build_connection_string(
                dest_conn.connection_type,
                dest_conn.server,
                dest_conn.database,
                dest_conn.username,
                dest_password,
                dest_conn.port
            )

            total_records = 0

            # Process each table mapping
            for table_mapping in workflow.table_mappings:
                records_processed = await self._process_table_mapping(
                    source_conn_str, dest_conn_str, table_mapping, execution_logs
                )
                total_records += records_processed

                execution_logs.append(
                    f"Processed table {table_mapping.source_table}: {records_processed} records"
                )

            # Mark execution as completed
            await update_workflow_execution(
                db,
                execution.id,
                WorkflowStatus.COMPLETED,
                records_processed=total_records,
                execution_logs=execution_logs
            )

            execution_logs.append(f"Workflow completed successfully. Total records: {total_records}")

        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            execution_logs.append(f"Workflow failed: {str(e)}")

            await update_workflow_execution(
                db,
                execution.id,
                WorkflowStatus.FAILED,
                error_message=str(e),
                execution_logs=execution_logs
            )

        # Reload and return execution
        execution = await db.get(WorkflowExecution, execution.id)
        return execution

    def _get_best_odbc_driver(self) -> str:
        """Get the best available ODBC driver for SQL Server"""
        if not PYODBC_AVAILABLE:
            raise ValueError("pyodbc is not available")

        available_drivers = pyodbc.drivers()
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server"
        ]

        for driver in preferred_drivers:
            if driver in available_drivers:
                return driver

        raise ValueError(f"No compatible SQL Server ODBC driver found. Available drivers: {', '.join(available_drivers)}")

    def _build_connection_string(
        self,
        connection_type: str,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int]
    ) -> str:
        """Build database connection string"""
        if connection_type in ["azure_sql", "sql_server"]:
            selected_driver = self._get_best_odbc_driver()

            conn_str = (
                f"DRIVER={{{selected_driver}}};"
                f"SERVER={server};"
                f"DATABASE={database or 'master'};"
                f"UID={username};"
                f"PWD={password}"
            )

            # Add encryption settings for Azure SQL and newer drivers
            if connection_type == "azure_sql" or "18" in selected_driver:
                conn_str += ";Encrypt=yes;TrustServerCertificate=yes"

            if port:
                conn_str = conn_str.replace(f"SERVER={server};", f"SERVER={server},{port};")
            return conn_str
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")

    async def _process_table_mapping(
        self,
        source_conn_str: str,
        dest_conn_str: str,
        table_mapping,
        execution_logs: List[str]
    ) -> int:
        """Process a single table mapping"""
        try:
            # Clear existing data from destination table
            logger.info(f"Clearing existing data from destination table: {table_mapping.destination_table}")
            execution_logs.append(f"Clearing existing data from destination table: {table_mapping.destination_table}")

            await self._clear_destination_table(dest_conn_str, table_mapping.destination_table)
            execution_logs.append(f"Successfully cleared destination table: {table_mapping.destination_table}")

            # Get PII columns that need masking
            pii_columns = [col for col in table_mapping.column_mappings if col.is_pii]

            # Build column lists for SELECT and INSERT
            source_columns = [col.source_column for col in table_mapping.column_mappings]
            dest_columns = [col.destination_column for col in table_mapping.column_mappings]

            # Process data in executor to avoid blocking
            loop = asyncio.get_event_loop()
            records_processed = await loop.run_in_executor(
                None,
                self._process_data_sync,
                source_conn_str,
                dest_conn_str,
                table_mapping,
                source_columns,
                dest_columns,
                execution_logs
            )

            return records_processed

        except Exception as e:
            logger.error(f"Failed to process table mapping {table_mapping.source_table}: {e}")
            raise

    def _process_data_sync(
        self,
        source_conn_str: str,
        dest_conn_str: str,
        table_mapping,
        source_columns: List[str],
        dest_columns: List[str],
        execution_logs: List[str]
    ) -> int:
        """Synchronous data processing for use with executor"""
        records_processed = 0

        with pyodbc.connect(source_conn_str, timeout=60) as source_conn:
            cursor = source_conn.cursor()

            # Build SELECT query
            select_query = f"SELECT {', '.join(source_columns)} FROM {table_mapping.source_table}"
            cursor.execute(select_query)

            # Fetch data in batches
            batch_size = 1000

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
                self._insert_masked_data_sync(
                    dest_conn_str, table_mapping.destination_table,
                    dest_columns, masked_rows
                )

                records_processed += len(masked_rows)

                # Log progress
                if records_processed % 5000 == 0:
                    execution_logs.append(
                        f"Processed batch for {table_mapping.source_table}: {records_processed} records so far"
                    )

        return records_processed

    async def _clear_destination_table(self, dest_conn_str: str, table_name: str):
        """Clear all data from destination table"""
        loop = asyncio.get_event_loop()

        def clear_sync():
            with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
                cursor = dest_conn.cursor()
                delete_query = f"DELETE FROM [{table_name}]"
                logger.info(f"Clearing all data from destination table: {table_name}")
                cursor.execute(delete_query)
                dest_conn.commit()
                logger.info(f"Cleared {cursor.rowcount} rows from table {table_name}")

        await loop.run_in_executor(None, clear_sync)

    def _insert_masked_data_sync(
        self,
        dest_conn_str: str,
        table_name: str,
        columns: List[str],
        data: List[List[Any]]
    ):
        """Synchronous insert of masked data"""
        with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
            cursor = dest_conn.cursor()

            # Build INSERT query
            placeholders = ', '.join(['?' for _ in columns])
            insert_query = f"INSERT INTO [{table_name}] ([{'], ['.join(columns)}]) VALUES ({placeholders})"

            # Execute batch insert
            cursor.executemany(insert_query, data)
            dest_conn.commit()

    def generate_sample_masked_data(
        self,
        pii_attribute: str,
        count: int = 5,
        sample_value: str = "sample"
    ) -> List[str]:
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

            return samples
        except Exception as e:
            logger.error(f"Failed to generate sample data for {pii_attribute}: {e}")
            return [f"Error generating sample: {str(e)}"] * count