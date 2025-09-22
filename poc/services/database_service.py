from azure.cosmos import exceptions as cosmos_exceptions
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import pyodbc
import logging
import os
from typing import List, Dict, Any, Optional
from models import DatabaseConnection, ConnectionType, ConnectionStatus
import json
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)

# Development mode local storage
DEV_MODE = os.getenv('DEVELOPMENT_MODE', 'False').lower() == 'true'
DEV_CONNECTIONS = []  # In-memory storage for connections in dev mode
DEV_SECRETS = {}      # In-memory storage for passwords in dev mode

class DatabaseService:
    """Service for managing database connections and operations"""

    def __init__(self, cosmos_client, keyvault_client: SecretClient):
        self.cosmos_client = cosmos_client
        self.keyvault_client = keyvault_client
        self.connections_container = self._get_or_create_container('testing') #testing is the container in Sample Cosmos DB

    def _get_or_create_container(self, container_name: str):
        """Get or create a Cosmos DB container"""
        if self.cosmos_client is None:
            logger.warning(f"No Cosmos DB client available - returning None for container {container_name}")
            return None

        try:
            container = self.cosmos_client.get_container_client(container_name)
            # Test if container exists
            container.read()
            return container
        except cosmos_exceptions.CosmosResourceNotFoundError:
            # Create container if it doesn't exist
            return self.cosmos_client.create_container_if_not_exists(
                id=container_name,
                partition_key={'paths': ['/id'], 'kind': 'Hash'}
            )
    async def save_connection(self, connection: DatabaseConnection) -> DatabaseConnection:
        """Save database connection to Cosmos DB and password to Key Vault"""
        try:
            # Generate ID if not provided
            if not connection.id:
                connection.id = str(uuid.uuid4())

            connection.updated_at = datetime.utcnow()

            if DEV_MODE:
                # In development mode, save to in-memory list
                # Check if connection already exists
                for i, conn in enumerate(DEV_CONNECTIONS):
                    if conn.id == connection.id:
                        # Update existing connection
                        DEV_CONNECTIONS[i] = connection
                        logger.info(f"DEVELOPMENT MODE: Connection {connection.name} updated in memory")
                        return connection

                # Add new connection
                DEV_CONNECTIONS.append(connection)
                logger.info(f"DEVELOPMENT MODE: Connection {connection.name} saved to memory")
                return connection
            else:
                # In production, save to Cosmos DB
                # Convert to dict for Cosmos DB
                connection_dict = connection.dict()

                # Save to Cosmos DB
                self.connections_container.upsert_item(connection_dict)

                logger.info(f"Connection {connection.name} saved successfully")
                return connection

        except Exception as e:
            logger.error(f"Failed to save connection: {e}")
            raise

    async def save_password_to_keyvault(self, password_key_name: str, password: str) -> str:
        """Save password to Azure Key Vault or local storage in dev mode"""
        if DEV_MODE:
            # In development mode, store password in memory
            DEV_SECRETS[password_key_name] = password
            logger.info(f"DEVELOPMENT MODE: Password saved to local storage with key: {password_key_name}")
            return password_key_name
        else:
            # In production, use Azure Key Vault
            try:
                if self.keyvault_client is None:
                    raise ValueError("Key Vault client not initialized")

                secret = self.keyvault_client.set_secret(password_key_name, password)
                logger.info(f"Password saved to Key Vault with name: {password_key_name}")
                return secret.name
            except Exception as e:
                logger.error(f"Failed to save password to Key Vault: {e}")
                raise

    async def get_password_from_keyvault(self, password_key_name: str) -> str:
        """Retrieve password from Azure Key Vault or local storage in dev mode"""
        if DEV_MODE:
            # In development mode, get password from memory
            password = DEV_SECRETS.get(password_key_name)
            if not password:
                logger.warning(f"Password not found in local storage: {password_key_name}")
                raise ValueError(f"Password not found: {password_key_name}")
            return password
        else:
            # In production, use Azure Key Vault
            try:
                if self.keyvault_client is None:
                    raise ValueError("Key Vault client not initialized")

                secret = self.keyvault_client.get_secret(password_key_name)
                return secret.value
            except Exception as e:
                logger.error(f"Failed to retrieve password from Key Vault: {e}")
                raise

    async def get_all_connections(self) -> List[DatabaseConnection]:
        """Retrieve all database connections"""
        if DEV_MODE:
            # In development mode, return from memory
            logger.info("DEVELOPMENT MODE: Returning connections from local storage")
            return DEV_CONNECTIONS
        else:
            # In production, use Cosmos DB
            try:
                if self.connections_container is None:
                    logger.warning("No Cosmos DB connection - returning empty list")
                    return []

                items = list(self.connections_container.read_all_items())
                connections = [DatabaseConnection(**item) for item in items]
                return connections
            except Exception as e:
                logger.error(f"Failed to retrieve connections: {e}")
                raise

    async def get_connection_by_id(self, connection_id: str) -> Optional[DatabaseConnection]:
        """Retrieve a specific database connection by ID"""
        if DEV_MODE:
            # In development mode, search in memory
            for conn in DEV_CONNECTIONS:
                if conn.id == connection_id:
                    return conn
            return None
        else:
            # In production, use Cosmos DB
            try:
                item = self.connections_container.read_item(
                    item=connection_id,
                    partition_key=connection_id
                )
                return DatabaseConnection(**item)
            except cosmos_exceptions.CosmosResourceNotFoundError:
                return None
            except Exception as e:
                logger.error(f"Failed to retrieve connection {connection_id}: {e}")
                raise
    async def delete_connection(self, connection_id: str) -> bool:
        """Delete a database connection"""
        if DEV_MODE:
            # In development mode, remove from memory
            for i, conn in enumerate(DEV_CONNECTIONS):
                if conn.id == connection_id:
                    DEV_CONNECTIONS.pop(i)
                    logger.info(f"DEVELOPMENT MODE: Connection {connection_id} deleted from memory")
                    return True
            logger.warning(f"Connection {connection_id} not found in memory")
            return False
        else:
            # In production, delete from Cosmos DB
            try:
                self.connections_container.delete_item(
                    item=connection_id,
                    partition_key=connection_id
                )
                logger.info(f"Connection {connection_id} deleted successfully")
                return True
            except cosmos_exceptions.CosmosResourceNotFoundError:
                return False
            except Exception as e:
                logger.error(f"Failed to delete connection {connection_id}: {e}")
                raise

    async def test_connection(self, connection: DatabaseConnection, password: str) -> tuple[bool, str]:
        """Test database connection"""
        try:
            if connection.connection_type == ConnectionType.AZURE_SQL:
                connection_string = self._build_azure_sql_connection_string(
                    connection, password
                )
            else:
                raise ValueError(f"Unsupported connection type: {connection.connection_type}")

            logger.info(f"Testing connection to {connection.server}/{connection.database}")

            # Test the connection
            try:
                with pyodbc.connect(connection_string, timeout=30) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

                return True, "Connection successful"
            except pyodbc.Error as db_err:
                # More specific database connection error
                error_msg = f"Database connection failed: {str(db_err)}"
                logger.error(error_msg)
                return False, error_msg

        except Exception as e:
            # Other errors (configuration, etc)
            error_msg = f"Connection setup failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg


    def _get_best_odbc_driver(self) -> str:
        """Get the best available ODBC driver for SQL Server"""
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

    def _build_azure_sql_connection_string(self, connection: DatabaseConnection, password: str) -> str:
        """Build Azure SQL connection string"""
        selected_driver = self._get_best_odbc_driver()
        port = connection.port or 1433

        connection_string = (
            f"DRIVER={{{selected_driver}}};"
            f"SERVER={connection.server},{port};"
            f"DATABASE={connection.database};"
            f"UID={connection.username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )

        # Add additional parameters if provided
        if connection.additional_params:
            for key, value in connection.additional_params.items():
                connection_string += f"{key}={value};"

        return connection_string

    async def get_tables(self, connection_id: str) -> List[str]:
        """Get list of tables from a database"""
        try:
            connection = await self.get_connection_by_id(connection_id)
            if not connection:
                raise ValueError(f"Connection {connection_id} not found")

            password = await self.get_password_from_keyvault(connection.password_key_vault_name)
            connection_string = self._build_azure_sql_connection_string(connection, password)

            with pyodbc.connect(connection_string, timeout=30) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """)
                tables = [row[0] for row in cursor.fetchall()]

            return tables

        except Exception as e:
            logger.error(f"Failed to get tables for connection {connection_id}: {e}")
            raise

    async def get_table_columns(self, connection_id: str, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        try:
            connection = await self.get_connection_by_id(connection_id)
            if not connection:
                raise ValueError(f"Connection {connection_id} not found")

            password = await self.get_password_from_keyvault(connection.password_key_vault_name)
            connection_string = self._build_azure_sql_connection_string(connection, password)

            with pyodbc.connect(connection_string, timeout=30) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, table_name)

                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        'name': row[0],
                        'data_type': row[1],
                        'is_nullable': row[2] == 'YES',
                        'max_length': row[3],
                        'precision': row[4],
                        'scale': row[5]
                    })

            return columns

        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            raise

 