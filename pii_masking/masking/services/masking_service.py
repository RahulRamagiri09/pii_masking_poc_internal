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

# Try to import psycopg2 for PostgreSQL support
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not installed. PostgreSQL connections will not be available.")

# Flag to track if pyodbc converter warning has been shown
_converter_warning_shown = False

from ..models.workflow import Workflow, WorkflowExecution, WorkflowStatus
from ..models.mapping import ColumnMapping
from ..crud.connection import decrypt_password
from ..crud.workflow import (
    get_workflow,
    update_workflow_execution,
    create_workflow_execution,
    update_workflow_status,
    get_workflow_execution_by_id
)
from ..crud.connection import get_connection

logger = logging.getLogger(__name__)


def handle_datetimeoffset(dto_value):
    """Convert DATETIMEOFFSET to string representation"""
    # Return the raw string representation
    return str(dto_value) if dto_value else None


def configure_pyodbc_converters():
    """Configure pyodbc to handle special SQL Server data types"""
    global _converter_warning_shown
    if PYODBC_AVAILABLE:
        try:
            # -155 is SQL_SS_TIMESTAMPOFFSET (DATETIMEOFFSET)
            # This may not be available in all pyodbc versions
            if hasattr(pyodbc, 'add_output_converter'):
                pyodbc.add_output_converter(-155, handle_datetimeoffset)
            else:
                if not _converter_warning_shown:
                    logger.warning("pyodbc.add_output_converter not available, DATETIMEOFFSET columns will be handled via SQL casting")
                    _converter_warning_shown = True
        except Exception as e:
            if not _converter_warning_shown:
                logger.warning(f"Failed to configure pyodbc converters: {e}")
                _converter_warning_shown = True


# Configure converters globally when module loads
if PYODBC_AVAILABLE:
    configure_pyodbc_converters()


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
        user_id: int,
        execution_id: int = None
    ) -> WorkflowExecution:
        """Execute a masking workflow"""
        # Check if required drivers are available
        if not PYODBC_AVAILABLE and not PSYCOPG2_AVAILABLE:
            raise RuntimeError(
                "No database drivers installed. Please install pyodbc for SQL Server or psycopg2 for PostgreSQL."
            )

        # Get existing execution record (should be created by API endpoint)
        if execution_id is None:
            # Fallback: create execution record if not provided (for backwards compatibility)
            execution = await create_workflow_execution(db, workflow_id, user_id)
        else:
            # Use the existing execution record
            execution = await get_workflow_execution_by_id(db, execution_id)
            if not execution:
                raise ValueError(f"Execution {execution_id} not found")

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

            # Add initial log entry and save to database immediately
            execution_logs.append(f"Workflow execution started for '{workflow.name}'")
            await update_workflow_execution(
                db,
                execution.id,
                WorkflowStatus.RUNNING,
                execution_logs=execution_logs
            )
            logger.info(f"Started execution {execution.id} for workflow {workflow.name}")

            # Get database connections
            source_conn = await get_connection(db, workflow.source_connection_id)
            dest_conn = await get_connection(db, workflow.destination_connection_id)

            if not source_conn or not dest_conn:
                raise ValueError("Source or destination connection not found")

            # Decrypt passwords
            try:
                source_password = decrypt_password(source_conn.password_encrypted)
                dest_password = decrypt_password(dest_conn.password_encrypted)
            except Exception as e:
                raise ValueError(f"Failed to decrypt connection passwords. Connections may have been created with a different encryption key. Error: {str(e)}")

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

            # Log connection establishment
            execution_logs.append(f"Database connections established successfully")
            await update_workflow_execution(
                db,
                execution.id,
                WorkflowStatus.RUNNING,
                execution_logs=execution_logs
            )

            total_records = 0

            # Process each table mapping
            table_count = len(workflow.table_mappings)
            execution_logs.append(f"Starting data masking for {table_count} table(s)")
            await update_workflow_execution(
                db,
                execution.id,
                WorkflowStatus.RUNNING,
                execution_logs=execution_logs
            )
            for idx, table_mapping in enumerate(workflow.table_mappings):
                # Log progress
                execution_logs.append(f"Processing table {table_mapping.source_table}...")

                records_processed = await self._process_table_mapping(
                    db, execution.id, source_conn_str, dest_conn_str, table_mapping, execution_logs, idx, table_count
                )
                total_records += records_processed

                execution_logs.append(
                    f"Processed table {table_mapping.source_table}: {records_processed} records"
                )

            # Mark execution as completed
            execution_logs.append(f"Workflow completed successfully. Total records: {total_records}")

            await update_workflow_execution(
                db,
                execution.id,
                WorkflowStatus.COMPLETED,
                records_processed=total_records,
                execution_logs=execution_logs
            )

            # Note: Workflow status will be updated by the background task executor
            # to ensure proper status tracking

        except Exception as e:
            logger.error(f"Workflow execution failed: {e}", exc_info=True)
            execution_logs.append(f"Workflow failed: {str(e)}")

            # Update execution status to failed
            try:
                await update_workflow_execution(
                    db,
                    execution.id,
                    WorkflowStatus.FAILED,
                    error_message=str(e),
                    execution_logs=execution_logs
                )
            except Exception as update_error:
                logger.error(f"Failed to update execution status: {update_error}")

            # Note: Workflow status will be updated by the Celery task
            # to avoid race conditions and duplicate status updates

            # Re-raise the exception so Celery task can handle it
            raise

        # Return execution object (no need to reload from database)
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
        elif connection_type == "postgresql":
            # PostgreSQL connection string format
            conn_str = f"postgresql://{username}:{password}@{server}"
            if port:
                conn_str += f":{port}"
            conn_str += f"/{database or 'postgres'}"
            return conn_str
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")

    def _get_connection_type(self, conn_str: str) -> str:
        """Determine connection type from connection string"""
        if conn_str.startswith("postgresql://"):
            return "postgresql"
        elif "DRIVER=" in conn_str:
            return "sql_server"
        else:
            raise ValueError(f"Cannot determine connection type from connection string")

    def _get_column_max_lengths(self, conn_str: str, table_name: str, columns: List[str]) -> Dict[str, Optional[int]]:
        """Get maximum character length for each column in the table"""
        conn_type = self._get_connection_type(conn_str)
        column_lengths = {}

        try:
            if conn_type == "sql_server":
                if not PYODBC_AVAILABLE:
                    return {col: None for col in columns}

                with pyodbc.connect(conn_str, timeout=60) as conn:
                    cursor = conn.cursor()

                    # Query to get column max lengths from SQL Server
                    query = """
                        SELECT c.name, c.max_length, t.name as type_name
                        FROM sys.columns c
                        INNER JOIN sys.tables tb ON c.object_id = tb.object_id
                        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                        WHERE tb.name = ? AND c.name IN ({})
                    """.format(','.join(['?' for _ in columns]))

                    cursor.execute(query, (table_name, *columns))
                    rows = cursor.fetchall()

                    for row in rows:
                        col_name, max_length, type_name = row[0], row[1], row[2]
                        # For nvarchar/nchar, max_length is in bytes (2 bytes per char)
                        if type_name in ['nvarchar', 'nchar']:
                            column_lengths[col_name] = max_length // 2 if max_length > 0 else None
                        elif type_name in ['varchar', 'char']:
                            column_lengths[col_name] = max_length if max_length > 0 else None
                        else:
                            column_lengths[col_name] = None

            elif conn_type == "postgresql":
                if not PSYCOPG2_AVAILABLE:
                    return {col: None for col in columns}

                with psycopg2.connect(conn_str) as conn:
                    cursor = conn.cursor()

                    # Query to get column max lengths from PostgreSQL
                    query = """
                        SELECT column_name, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_name = %s AND column_name = ANY(%s)
                    """

                    cursor.execute(query, (table_name, columns))
                    rows = cursor.fetchall()

                    for row in rows:
                        col_name, max_length = row[0], row[1]
                        column_lengths[col_name] = max_length

            # Set None for columns not found in metadata
            for col in columns:
                if col not in column_lengths:
                    column_lengths[col] = None

            logger.info(f"Column max lengths for {table_name}: {column_lengths}")

        except Exception as e:
            logger.warning(f"Could not get column lengths for {table_name}: {e}")
            # Return None for all columns if query fails
            return {col: None for col in columns}

        return column_lengths

    async def _process_table_mapping(
        self,
        db: AsyncSession,
        execution_id: int,
        source_conn_str: str,
        dest_conn_str: str,
        table_mapping,
        execution_logs: List[str],
        table_index: int = 0,
        total_tables: int = 1
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

            # Get column max lengths from destination table
            column_max_lengths = self._get_column_max_lengths(
                dest_conn_str,
                table_mapping.destination_table,
                dest_columns
            )

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
                execution_logs,
                column_max_lengths,
                db,
                execution_id,
                loop
            )

            # Log completion
            logger.info(f"Completed table {table_mapping.source_table}: {records_processed} records")

            return records_processed

        except Exception as e:
            logger.error(f"Failed to process table mapping {table_mapping.source_table}: {e}")
            raise

    def _update_logs_sync(self, db: AsyncSession, execution_id: int, execution_logs: List[str], records_processed: int, loop):
        """Helper to update execution logs from sync context"""
        if db and execution_id and loop:
            try:
                # Schedule the coroutine in the event loop from this thread
                future = asyncio.run_coroutine_threadsafe(
                    update_workflow_execution(
                        db,
                        execution_id,
                        WorkflowStatus.RUNNING,
                        records_processed=records_processed,
                        execution_logs=execution_logs.copy()  # Copy to avoid list mutation issues
                    ),
                    loop
                )
                # Wait for the update to complete (with timeout)
                future.result(timeout=10)
                logger.debug(f"Updated execution logs: {len(execution_logs)} entries, {records_processed} records")
            except Exception as e:
                logger.warning(f"Failed to update execution logs in real-time: {e}")

    def _process_data_sync(
        self,
        source_conn_str: str,
        dest_conn_str: str,
        table_mapping,
        source_columns: List[str],
        dest_columns: List[str],
        execution_logs: List[str],
        column_max_lengths: Dict[str, Optional[int]] = None,
        db: AsyncSession = None,
        execution_id: int = None,
        loop = None
    ) -> int:
        """Synchronous data processing for use with executor"""
        source_conn_type = self._get_connection_type(source_conn_str)

        records_processed = 0

        if source_conn_type == "sql_server":
            # SQL Server processing using pyodbc
            with pyodbc.connect(source_conn_str, timeout=60) as source_conn:
                cursor = source_conn.cursor()

                # Get total count for progress tracking
                count_query = f"SELECT COUNT(*) FROM {table_mapping.source_table}"
                cursor.execute(count_query)
                total_count = cursor.fetchone()[0]
                logger.info(f"Total records in source table {table_mapping.source_table}: {total_count}")
                execution_logs.append(f"Starting to process {total_count} records from {table_mapping.source_table}")

                if total_count == 0:
                    logger.warning(f"Source table {table_mapping.source_table} is empty")
                    execution_logs.append(f"Warning: Source table {table_mapping.source_table} is empty")
                    return 0

                # Build SELECT query with type casting for problematic data types
                casted_columns = []
                for col in source_columns:
                    casted_columns.append(f"TRY_CAST([{col}] AS NVARCHAR(MAX)) AS [{col}]")

                select_query = f"SELECT {', '.join(casted_columns)} FROM {table_mapping.source_table}"
                cursor.execute(select_query)

                # Fetch data in batches
                batch_size = 1000
                batch_num = 0

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    batch_num += 1
                    logger.info(f"Processing batch {batch_num}: {len(rows)} rows")

                    # Process and mask data
                    masked_rows = self._mask_rows(rows, table_mapping)

                    # Insert masked data into destination
                    self._insert_masked_data_sync(
                        dest_conn_str, table_mapping.destination_table,
                        dest_columns, masked_rows
                    )

                    records_processed += len(masked_rows)

                    # Log progress after each batch
                    progress_pct = (records_processed / total_count * 100) if total_count > 0 else 100
                    execution_logs.append(
                        f"Processing batch {batch_num} for {table_mapping.source_table}: {records_processed}/{total_count} records ({progress_pct:.1f}%)"
                    )

                    # Update database with current progress
                    self._update_logs_sync(db, execution_id, execution_logs, records_processed, loop)

        elif source_conn_type == "postgresql":
            # PostgreSQL processing using psycopg2
            try:
                logger.info(f"Connecting to source PostgreSQL database...")
                with psycopg2.connect(source_conn_str) as source_conn:
                    cursor = source_conn.cursor()

                    # Build SELECT query - keep original types, PostgreSQL driver handles conversion
                    quoted_columns = [f'"{col}"' for col in source_columns]
                    select_query = f"SELECT {', '.join(quoted_columns)} FROM {table_mapping.source_table}"
                    logger.info(f"Executing query: {select_query}")
                    execution_logs.append(f"Fetching data from source table: {table_mapping.source_table}")

                    cursor.execute(select_query)

                    # Get total count for logging
                    count_query = f"SELECT COUNT(*) FROM {table_mapping.source_table}"
                    cursor.execute(count_query)
                    total_count = cursor.fetchone()[0]
                    logger.info(f"Total records in source table {table_mapping.source_table}: {total_count}")
                    execution_logs.append(f"Starting to process {total_count} records from {table_mapping.source_table}")

                    if total_count == 0:
                        logger.warning(f"Source table {table_mapping.source_table} is empty")
                        execution_logs.append(f"Warning: Source table {table_mapping.source_table} is empty")
                        return 0

                    # Re-execute the select query since we used cursor for count
                    cursor.execute(select_query)

                    # Fetch data in batches
                    batch_size = 1000
                    batch_num = 0

                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break

                        batch_num += 1
                        logger.info(f"Processing batch {batch_num}: {len(rows)} rows")

                        # Process and mask data
                        logger.info(f"Starting masking for batch {batch_num}...")
                        masked_rows = self._mask_rows(rows, table_mapping)
                        logger.info(f"Masking completed for batch {batch_num}, got {len(masked_rows)} masked rows")

                        # Insert masked data into destination
                        try:
                            self._insert_masked_data_sync(
                                dest_conn_str, table_mapping.destination_table,
                                dest_columns, masked_rows
                            )
                            logger.info(f"Successfully inserted batch {batch_num} into destination")
                        except Exception as insert_error:
                            error_msg = f"Failed to insert batch {batch_num}: {str(insert_error)}"
                            logger.error(error_msg)
                            execution_logs.append(error_msg)
                            raise

                        records_processed += len(masked_rows)

                        # Log progress after each batch
                        progress_pct = (records_processed / total_count * 100) if total_count > 0 else 100
                        execution_logs.append(
                            f"Processing batch {batch_num} for {table_mapping.source_table}: {records_processed}/{total_count} records ({progress_pct:.1f}%)"
                        )

                        # Update database with current progress
                        self._update_logs_sync(db, execution_id, execution_logs, records_processed, loop)

                    logger.info(f"Completed processing {records_processed} records from {table_mapping.source_table}")
                    execution_logs.append(f"Successfully processed {records_processed} records")

            except psycopg2.Error as pg_error:
                error_msg = f"PostgreSQL error processing table {table_mapping.source_table}: {str(pg_error)}"
                logger.error(error_msg)
                execution_logs.append(error_msg)
                raise
            except Exception as e:
                error_msg = f"Unexpected error processing table {table_mapping.source_table}: {str(e)}"
                logger.error(error_msg)
                execution_logs.append(error_msg)
                raise

        return records_processed

    def _mask_rows(self, rows, table_mapping) -> List[List[Any]]:
        """Mask PII data in rows"""
        masked_rows = []
        total_rows = len(rows)

        for row_num, row in enumerate(rows, 1):
            if row_num % 25 == 0:  # Log every 25 rows
                logger.info(f"Masking progress: {row_num}/{total_rows} rows")

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
                else:
                    # For non-PII columns, keep original value with proper type
                    # This ensures integers stay integers, dates stay dates, etc.
                    pass

            masked_rows.append(masked_row)

        logger.info(f"Masking completed: {total_rows}/{total_rows} rows masked")
        return masked_rows

    def _get_identity_columns(self, dest_conn_str: str, table_name: str) -> List[str]:
        """Get list of identity columns for a table"""
        conn_type = self._get_connection_type(dest_conn_str)
        identity_columns = []

        try:
            if conn_type == "sql_server":
                if not PYODBC_AVAILABLE:
                    return []

                with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
                    cursor = dest_conn.cursor()

                    # Query to find identity columns using sys.columns and sys.tables
                    identity_query = """
                        SELECT c.name AS COLUMN_NAME
                        FROM sys.columns c
                        INNER JOIN sys.tables t ON c.object_id = t.object_id
                        WHERE t.name = ? AND c.is_identity = 1
                    """

                    cursor.execute(identity_query, (table_name,))
                    rows = cursor.fetchall()
                    identity_columns = [row[0] for row in rows]

            elif conn_type == "postgresql":
                if not PSYCOPG2_AVAILABLE:
                    return []

                with psycopg2.connect(dest_conn_str) as dest_conn:
                    cursor = dest_conn.cursor()

                    # Query to find SERIAL/IDENTITY columns in PostgreSQL
                    identity_query = """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                        AND (column_default LIKE 'nextval%%' OR is_identity = 'YES')
                    """

                    logger.info(f"Detecting identity columns for table: {table_name}")
                    cursor.execute(identity_query, (table_name,))
                    rows = cursor.fetchall()
                    identity_columns = [row[0] for row in rows]
                    logger.info(f"Detected identity columns: {identity_columns}")

            if identity_columns:
                logger.info(f"Found identity columns in table {table_name}: {identity_columns}")
            else:
                logger.info(f"No identity columns found in table {table_name}")

        except Exception as e:
            logger.error(f"Error detecting identity columns for table {table_name}: {e}")
            # Return empty list to allow insert to proceed (may fail but won't crash detection)
            return []

        return identity_columns

    async def _clear_destination_table(self, dest_conn_str: str, table_name: str):
        """Clear all data from destination table"""
        loop = asyncio.get_event_loop()
        conn_type = self._get_connection_type(dest_conn_str)

        def clear_sync():
            if conn_type == "sql_server":
                with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
                    cursor = dest_conn.cursor()
                    delete_query = f"DELETE FROM [{table_name}]"
                    logger.info(f"Clearing all data from destination table: {table_name}")
                    cursor.execute(delete_query)
                    dest_conn.commit()
                    logger.info(f"Cleared {cursor.rowcount} rows from table {table_name}")
            elif conn_type == "postgresql":
                try:
                    logger.info(f"Connecting to destination PostgreSQL to clear table: {table_name}")
                    with psycopg2.connect(dest_conn_str) as dest_conn:
                        cursor = dest_conn.cursor()
                        delete_query = f'DELETE FROM "{table_name}"'
                        logger.info(f"Executing: {delete_query}")
                        cursor.execute(delete_query)
                        rows_deleted = cursor.rowcount
                        dest_conn.commit()
                        logger.info(f"Successfully cleared {rows_deleted} rows from table {table_name}")
                except psycopg2.Error as pg_error:
                    logger.error(f"PostgreSQL error clearing table {table_name}: {pg_error}")
                    raise
                except Exception as e:
                    logger.error(f"Error clearing table {table_name}: {str(e)}")
                    raise

        await loop.run_in_executor(None, clear_sync)

    def _insert_masked_data_sync(
        self,
        dest_conn_str: str,
        table_name: str,
        columns: List[str],
        data: List[List[Any]]
    ):
        """Synchronous insert of masked data"""
        conn_type = self._get_connection_type(dest_conn_str)

        if conn_type == "sql_server":
            with pyodbc.connect(dest_conn_str, timeout=60) as dest_conn:
                cursor = dest_conn.cursor()

                # Detect and exclude identity columns
                identity_columns = self._get_identity_columns(dest_conn_str, table_name)
                logger.info(f"Identity columns detected: {identity_columns}")

                # Filter out identity columns
                filtered_columns = [col for col in columns if col not in identity_columns]

                # Get column indices to filter data
                identity_indices = [i for i, col in enumerate(columns) if col in identity_columns]

                # Filter data rows
                filtered_data = []
                for row in data:
                    filtered_row = [row[i] for i in range(len(row)) if i not in identity_indices]
                    filtered_data.append(filtered_row)

                logger.info(f"Using {len(filtered_columns)} columns (excluded {len(identity_columns)} identity columns)")

                # Build INSERT query without identity columns
                placeholders = ', '.join(['?' for _ in filtered_columns])
                insert_query = f"INSERT INTO [{table_name}] ([{'], ['.join(filtered_columns)}]) VALUES ({placeholders})"

                logger.info(f"Inserting {len(filtered_data)} rows into {table_name}")

                # Execute batch insert
                cursor.executemany(insert_query, filtered_data)
                dest_conn.commit()

                logger.info(f"Successfully inserted {len(filtered_data)} rows")

        elif conn_type == "postgresql":
            dest_conn = None
            try:
                logger.info(f"Connecting to destination PostgreSQL database for insert...")

                # Detect and exclude identity/serial columns
                identity_columns = self._get_identity_columns(dest_conn_str, table_name)
                logger.info(f"Identity columns detected: {identity_columns}")

                # Filter out identity columns
                filtered_columns = [col for col in columns if col not in identity_columns]

                # Get column indices to filter data
                identity_indices = [i for i, col in enumerate(columns) if col in identity_columns]

                # Filter data rows
                filtered_data = []
                for row in data:
                    filtered_row = [row[i] for i in range(len(row)) if i not in identity_indices]
                    filtered_data.append(filtered_row)

                logger.info(f"Columns after filtering: {filtered_columns}")
                logger.info(f"Using {len(filtered_columns)} columns (excluded {len(identity_columns)} identity columns)")

                # Create connection with proper error handling
                dest_conn = psycopg2.connect(dest_conn_str, connect_timeout=30)
                cursor = dest_conn.cursor()

                # Build INSERT query without identity columns
                placeholders = ', '.join(['%s' for _ in filtered_columns])
                quoted_columns = ', '.join([f'"{col}"' for col in filtered_columns])
                insert_query = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'

                logger.info(f"Inserting {len(filtered_data)} rows into {table_name}")
                logger.debug(f"INSERT query: {insert_query}")
                logger.debug(f"Sample filtered row: {filtered_data[0] if filtered_data else 'No data'}")

                # Use simple executemany
                logger.info(f"Starting executemany for {len(filtered_data)} rows...")
                cursor.executemany(insert_query, filtered_data)
                logger.info(f"Executemany completed, committing...")

                # Commit the transaction
                dest_conn.commit()
                logger.info(f"Committed successfully - {len(filtered_data)} rows inserted into {table_name}")

            except psycopg2.Error as pg_error:
                error_msg = f"PostgreSQL insert error for table {table_name}: {pg_error}"
                logger.error(error_msg)
                if 'insert_query' in locals():
                    logger.error(f"Query: {insert_query}")
                if 'filtered_data' in locals() and filtered_data:
                    logger.error(f"Sample data: {filtered_data[0]}")

                # Rollback on error
                if dest_conn:
                    try:
                        dest_conn.rollback()
                        logger.info("Transaction rolled back")
                    except:
                        pass
                raise
            except Exception as e:
                error_msg = f"Unexpected insert error for table {table_name}: {str(e)}"
                logger.error(error_msg, exc_info=True)

                # Rollback on error
                if dest_conn:
                    try:
                        dest_conn.rollback()
                        logger.info("Transaction rolled back")
                    except:
                        pass
                raise
            finally:
                # Close connection
                if dest_conn:
                    try:
                        dest_conn.close()
                    except:
                        pass

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