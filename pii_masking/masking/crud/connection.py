from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional, List
from cryptography.fernet import Fernet
import os
from ..models.connection import DatabaseConnection, ConnectionStatus
from ..schemas.connection import ConnectionCreate, ConnectionUpdate
from ...core.config import settings

# Try to import pyodbc, but make it optional
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

# Try to import asyncpg for PostgreSQL
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False


# Generate or get encryption key
ENCRYPTION_KEY = settings.ENCRYPTION_KEY or Fernet.generate_key().decode()
cipher = Fernet(ENCRYPTION_KEY.encode())


def encrypt_password(password: str) -> str:
    """Encrypt password for storage"""
    return cipher.encrypt(password.encode()).decode()


def decrypt_password(encrypted_password: str) -> str:
    """Decrypt password for use"""
    return cipher.decrypt(encrypted_password.encode()).decode()


async def create_connection(
    db: AsyncSession,
    connection: ConnectionCreate,
    user_id: int,
    created_by: int = None
) -> DatabaseConnection:
    """Create a new database connection"""
    encrypted_password = encrypt_password(connection.password)

    # Test connection before saving (like POC does)
    test_result, test_message = await test_connection({
        "connection_type": connection.connection_type.value,
        "server": connection.server,
        "database": connection.database,
        "username": connection.username,
        "password": connection.password,
        "port": connection.port
    })

    # Set status based on test result
    from ..models.connection import ConnectionStatus
    status = ConnectionStatus.ACTIVE.value if test_result else ConnectionStatus.ERROR.value

    db_connection = DatabaseConnection(
        name=connection.name,
        connection_type=connection.connection_type.value,
        server=connection.server,
        database=connection.database,
        username=connection.username,
        password_encrypted=encrypted_password,
        port=connection.port,
        additional_params=connection.additional_params,
        status=status,
        test_connection_result=test_message,
        user_id=user_id,
        created_by=created_by or user_id
    )

    db.add(db_connection)
    await db.commit()
    await db.refresh(db_connection)

    # Load with relationships
    result = await db.execute(
        select(DatabaseConnection)
        .options(selectinload(DatabaseConnection.user))
        .where(DatabaseConnection.id == db_connection.id)
    )
    return result.scalar_one()


async def get_connection(db: AsyncSession, connection_id: int) -> Optional[DatabaseConnection]:
    """Get a connection by ID"""
    result = await db.execute(
        select(DatabaseConnection)
        .options(selectinload(DatabaseConnection.user))
        .where(DatabaseConnection.id == connection_id)
    )
    return result.scalar_one_or_none()


async def get_connections(
    db: AsyncSession,
    user_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100
) -> List[DatabaseConnection]:
    """Get all connections, optionally filtered by user"""
    query = select(DatabaseConnection).options(selectinload(DatabaseConnection.user))

    if user_id:
        query = query.where(DatabaseConnection.user_id == user_id)

    query = query.order_by(DatabaseConnection.id).offset(skip).limit(limit)
    result = await db.execute(query)
    return result.scalars().all()


async def update_connection(
    db: AsyncSession,
    connection_id: int,
    connection_update: ConnectionUpdate,
    updated_by: int = None
) -> Optional[DatabaseConnection]:
    """Update a database connection"""
    result = await db.execute(
        select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
    )
    db_connection = result.scalar_one_or_none()

    if db_connection:
        update_data = connection_update.model_dump(exclude_unset=True)

        # Encrypt password if provided
        if "password" in update_data:
            update_data["password_encrypted"] = encrypt_password(update_data.pop("password"))

        # Handle connection_type enum
        if "connection_type" in update_data:
            update_data["connection_type"] = update_data["connection_type"].value

        # Handle status enum
        if "status" in update_data:
            update_data["status"] = update_data["status"].value

        for field, value in update_data.items():
            setattr(db_connection, field, value)

        if updated_by is not None:
            db_connection.updated_by = updated_by

        await db.commit()
        await db.refresh(db_connection)

        # Load with relationships
        result = await db.execute(
            select(DatabaseConnection)
            .options(selectinload(DatabaseConnection.user))
            .where(DatabaseConnection.id == connection_id)
        )
        return result.scalar_one()

    return None


async def delete_connection(db: AsyncSession, connection_id: int) -> bool:
    """Delete a database connection (soft delete)"""
    result = await db.execute(
        select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
    )
    db_connection = result.scalar_one_or_none()

    if db_connection:
        db_connection.is_active = False
        await db.commit()
        return True

    return False


async def get_source_tables(db: AsyncSession, connection_id: int) -> tuple[bool, List[str], str]:
    """Get list of source tables from a database connection"""
    import asyncio

    # Get connection details
    connection = await get_connection(db, connection_id)
    if not connection:
        return False, [], "Connection not found"

    # Decrypt password for use
    try:
        password = decrypt_password(connection.password_encrypted)
    except Exception:
        return False, [], "Failed to decrypt password. This connection was encrypted with a different key."

    connection_type = connection.connection_type

    try:
        if connection_type == "postgresql":
            # Use asyncpg for PostgreSQL
            if not ASYNCPG_AVAILABLE:
                return False, [], "asyncpg is not installed. Please install it with: pip install asyncpg"

            # Build PostgreSQL connection string
            port = connection.port or 5432
            database = connection.database or 'postgres'

            conn = await asyncpg.connect(
                host=connection.server,
                port=port,
                database=database,
                user=connection.username,
                password=password,
                timeout=10
            )

            # Get tables query
            result = await conn.fetch("""
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY tablename
            """)
            await conn.close()

            tables = [row['tablename'] for row in result]
            return True, tables, "Source tables retrieved successfully"

        elif connection_type in ["azure_sql", "sql_server"]:
            # Use pyodbc for SQL Server/Azure SQL
            if not PYODBC_AVAILABLE:
                return False, [], "pyodbc is not installed. Please install Microsoft C++ Build Tools and pyodbc."

            # Dynamically find the best available ODBC driver
            available_drivers = pyodbc.drivers()
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server"
            ]

            selected_driver = None
            for driver in preferred_drivers:
                if driver in available_drivers:
                    selected_driver = driver
                    break

            if not selected_driver:
                return False, [], f"No compatible SQL Server ODBC driver found. Available drivers: {', '.join(available_drivers)}"

            # Handle server and port properly
            server = connection.server
            port = connection.port

            # Check if server already includes port (format: server,port or server:port)
            if port and ',' not in server and ':' not in server:
                server_with_port = f"{server},{port}"
            else:
                server_with_port = server

            conn_str = (
                f"DRIVER={{{selected_driver}}};"
                f"SERVER={server_with_port};"
                f"DATABASE={connection.database or 'master'};"
                f"UID={connection.username};"
                f"PWD={password};"
                f"Connection Timeout=30;"
                f"Command Timeout=30"
            )

            # Add encryption settings for Azure SQL and newer drivers
            if connection_type == "azure_sql" or "18" in selected_driver:
                conn_str += ";Encrypt=yes;TrustServerCertificate=yes"

            # Test connection in executor to avoid blocking
            loop = asyncio.get_event_loop()

            def get_source_tables_sync():
                try:
                    with pyodbc.connect(conn_str, timeout=30) as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE'
                            ORDER BY TABLE_NAME
                        """)
                        tables = [row[0] for row in cursor.fetchall()]
                        return True, tables, "Source tables retrieved successfully"

                except pyodbc.Error as e:
                    error_msg = str(e)
                    if "timeout" in error_msg.lower():
                        return False, [], f"Connection timeout: {error_msg}"
                    elif "login" in error_msg.lower():
                        return False, [], f"Authentication failed: {error_msg}"
                    elif "network" in error_msg.lower() or "tcp" in error_msg.lower():
                        return False, [], f"Network error: {error_msg}"
                    else:
                        return False, [], f"Database error: {error_msg}"

            result = await loop.run_in_executor(None, get_source_tables_sync)
            return result

        else:
            return False, [], f"Unsupported connection type: {connection_type}. Supported types: postgresql, azure_sql, sql_server"

    except Exception as e:
        return False, [], f"Failed to retrieve tables: {str(e)}"


async def get_destination_tables(db: AsyncSession, connection_id: int) -> tuple[bool, List[str], str]:
    """Get list of destination tables from a database connection"""
    import asyncio

    # Get connection details
    connection = await get_connection(db, connection_id)
    if not connection:
        return False, [], "Connection not found"

    # Decrypt password for use
    try:
        password = decrypt_password(connection.password_encrypted)
    except Exception:
        return False, [], "Failed to decrypt password. This connection was encrypted with a different key."

    connection_type = connection.connection_type

    try:
        if connection_type == "postgresql":
            # Use asyncpg for PostgreSQL
            if not ASYNCPG_AVAILABLE:
                return False, [], "asyncpg is not installed. Please install it with: pip install asyncpg"

            # Build PostgreSQL connection string
            port = connection.port or 5432
            database = connection.database or 'postgres'

            conn = await asyncpg.connect(
                host=connection.server,
                port=port,
                database=database,
                user=connection.username,
                password=password,
                timeout=10
            )

            # Get tables query
            result = await conn.fetch("""
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY tablename
            """)
            await conn.close()

            tables = [row['tablename'] for row in result]
            return True, tables, "Destination tables retrieved successfully"

        elif connection_type in ["azure_sql", "sql_server"]:
            # Use pyodbc for SQL Server/Azure SQL
            if not PYODBC_AVAILABLE:
                return False, [], "pyodbc is not installed. Please install Microsoft C++ Build Tools and pyodbc."

            # Dynamically find the best available ODBC driver
            available_drivers = pyodbc.drivers()
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server"
            ]

            selected_driver = None
            for driver in preferred_drivers:
                if driver in available_drivers:
                    selected_driver = driver
                    break

            if not selected_driver:
                return False, [], f"No compatible SQL Server ODBC driver found. Available drivers: {', '.join(available_drivers)}"

            # Handle server and port properly
            server = connection.server
            port = connection.port

            # Check if server already includes port (format: server,port or server:port)
            if port and ',' not in server and ':' not in server:
                server_with_port = f"{server},{port}"
            else:
                server_with_port = server

            conn_str = (
                f"DRIVER={{{selected_driver}}};"
                f"SERVER={server_with_port};"
                f"DATABASE={connection.database or 'master'};"
                f"UID={connection.username};"
                f"PWD={password};"
                f"Connection Timeout=30;"
                f"Command Timeout=30"
            )

            # Add encryption settings for Azure SQL and newer drivers
            if connection_type == "azure_sql" or "18" in selected_driver:
                conn_str += ";Encrypt=yes;TrustServerCertificate=yes"

            # Test connection in executor to avoid blocking
            loop = asyncio.get_event_loop()

            def get_destination_tables_sync():
                try:
                    with pyodbc.connect(conn_str, timeout=30) as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE'
                            ORDER BY TABLE_NAME
                        """)
                        tables = [row[0] for row in cursor.fetchall()]
                        return True, tables, "Destination tables retrieved successfully"

                except pyodbc.Error as e:
                    error_msg = str(e)
                    if "timeout" in error_msg.lower():
                        return False, [], f"Connection timeout: {error_msg}"
                    elif "login" in error_msg.lower():
                        return False, [], f"Authentication failed: {error_msg}"
                    elif "network" in error_msg.lower() or "tcp" in error_msg.lower():
                        return False, [], f"Network error: {error_msg}"
                    else:
                        return False, [], f"Database error: {error_msg}"

            result = await loop.run_in_executor(None, get_destination_tables_sync)
            return result

        else:
            return False, [], f"Unsupported connection type: {connection_type}. Supported types: postgresql, azure_sql, sql_server"

    except Exception as e:
        return False, [], f"Failed to retrieve tables: {str(e)}"


async def get_table_columns(db: AsyncSession, connection_id: int, table_name: str) -> tuple[bool, List[dict], str]:
    """Get column information for a specific table from a database connection"""
    import asyncio

    # Get connection details
    connection = await get_connection(db, connection_id)
    if not connection:
        return False, [], "Connection not found"

    # Decrypt password for use
    try:
        password = decrypt_password(connection.password_encrypted)
    except Exception:
        return False, [], "Failed to decrypt password. This connection was encrypted with a different key."

    connection_type = connection.connection_type

    try:
        if connection_type == "postgresql":
            # Use asyncpg for PostgreSQL
            if not ASYNCPG_AVAILABLE:
                return False, [], "asyncpg is not installed. Please install it with: pip install asyncpg"

            # Build PostgreSQL connection string
            port = connection.port or 5432
            database = connection.database or 'postgres'

            conn = await asyncpg.connect(
                host=connection.server,
                port=port,
                database=database,
                user=connection.username,
                password=password,
                timeout=10
            )

            # Get columns query
            result = await conn.fetch("""
                SELECT
                    column_name as name,
                    data_type,
                    is_nullable,
                    character_maximum_length as max_length,
                    numeric_precision as precision,
                    numeric_scale as scale
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, table_name)
            await conn.close()

            columns = []
            for row in result:
                columns.append({
                    'name': row['name'],
                    'data_type': row['data_type'],
                    'is_nullable': row['is_nullable'] == 'YES',
                    'max_length': row['max_length'],
                    'precision': row['precision'],
                    'scale': row['scale']
                })
            return True, columns, "Table columns retrieved successfully"

        elif connection_type in ["azure_sql", "sql_server"]:
            # Use pyodbc for SQL Server/Azure SQL
            if not PYODBC_AVAILABLE:
                return False, [], "pyodbc is not installed. Please install Microsoft C++ Build Tools and pyodbc."

            # Dynamically find the best available ODBC driver
            available_drivers = pyodbc.drivers()
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server"
            ]

            selected_driver = None
            for driver in preferred_drivers:
                if driver in available_drivers:
                    selected_driver = driver
                    break

            if not selected_driver:
                return False, [], f"No compatible SQL Server ODBC driver found. Available drivers: {', '.join(available_drivers)}"

            # Handle server and port properly
            server = connection.server
            port = connection.port

            # Check if server already includes port (format: server,port or server:port)
            if port and ',' not in server and ':' not in server:
                server_with_port = f"{server},{port}"
            else:
                server_with_port = server

            conn_str = (
                f"DRIVER={{{selected_driver}}};"
                f"SERVER={server_with_port};"
                f"DATABASE={connection.database or 'master'};"
                f"UID={connection.username};"
                f"PWD={password};"
                f"Connection Timeout=30;"
                f"Command Timeout=30"
            )

            # Add encryption settings for Azure SQL and newer drivers
            if connection_type == "azure_sql" or "18" in selected_driver:
                conn_str += ";Encrypt=yes;TrustServerCertificate=yes"

            # Test connection in executor to avoid blocking
            loop = asyncio.get_event_loop()

            def get_table_columns_sync():
                try:
                    with pyodbc.connect(conn_str, timeout=30) as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT
                                COLUMN_NAME as name,
                                DATA_TYPE as data_type,
                                IS_NULLABLE,
                                CHARACTER_MAXIMUM_LENGTH as max_length,
                                NUMERIC_PRECISION as precision,
                                NUMERIC_SCALE as scale
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
                                'max_length': row[3] if row[3] != -1 else None,
                                'precision': row[4],
                                'scale': row[5]
                            })
                        return True, columns, "Table columns retrieved successfully"

                except pyodbc.Error as e:
                    error_msg = str(e)
                    if "timeout" in error_msg.lower():
                        return False, [], f"Connection timeout: {error_msg}"
                    elif "login" in error_msg.lower():
                        return False, [], f"Authentication failed: {error_msg}"
                    elif "network" in error_msg.lower() or "tcp" in error_msg.lower():
                        return False, [], f"Network error: {error_msg}"
                    elif "invalid object name" in error_msg.lower() or "table" in error_msg.lower():
                        return False, [], f"Table '{table_name}' not found in database"
                    else:
                        return False, [], f"Database error: {error_msg}"

            result = await loop.run_in_executor(None, get_table_columns_sync)
            return result

        else:
            return False, [], f"Unsupported connection type: {connection_type}. Supported types: postgresql, azure_sql, sql_server"

    except Exception as e:
        return False, [], f"Failed to retrieve table columns: {str(e)}"


async def test_connection(connection_params: dict) -> tuple[bool, str]:
    """Test a database connection"""
    import asyncio

    connection_type = connection_params["connection_type"]

    try:
        if connection_type == "postgresql":
            # Use asyncpg for PostgreSQL
            if not ASYNCPG_AVAILABLE:
                return False, "asyncpg is not installed. Please install it with: pip install asyncpg"

            # Build PostgreSQL connection string
            port = connection_params.get('port', 5432)
            database = connection_params.get('database', 'postgres')

            conn = await asyncpg.connect(
                host=connection_params['server'],
                port=port,
                database=database,
                user=connection_params['username'],
                password=connection_params['password'],
                timeout=10
            )

            # Test query
            result = await conn.fetchval('SELECT 1')
            await conn.close()

            if result == 1:
                return True, "PostgreSQL connection successful"
            else:
                return False, "PostgreSQL connection test failed"

        elif connection_type in ["azure_sql", "sql_server"]:
            # Use pyodbc for SQL Server/Azure SQL
            if not PYODBC_AVAILABLE:
                return False, "pyodbc is not installed. Please install Microsoft C++ Build Tools and pyodbc."

            # Dynamically find the best available ODBC driver
            available_drivers = pyodbc.drivers()
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server"
            ]

            selected_driver = None
            for driver in preferred_drivers:
                if driver in available_drivers:
                    selected_driver = driver
                    break

            if not selected_driver:
                return False, f"No compatible SQL Server ODBC driver found. Available drivers: {', '.join(available_drivers)}"

            # Handle server and port properly
            server = connection_params['server']
            port = connection_params.get('port')

            # Check if server already includes port (format: server,port or server:port)
            if port and ',' not in server and ':' not in server:
                server_with_port = f"{server},{port}"
            else:
                server_with_port = server

            conn_str = (
                f"DRIVER={{{selected_driver}}};"
                f"SERVER={server_with_port};"
                f"DATABASE={connection_params.get('database', 'master')};"
                f"UID={connection_params['username']};"
                f"PWD={connection_params['password']};"
                f"Connection Timeout=30;"
                f"Command Timeout=30"
            )

            # Add encryption settings for Azure SQL and newer drivers
            if connection_type == "azure_sql" or "18" in selected_driver:
                conn_str += ";Encrypt=yes;TrustServerCertificate=yes"

            # Test connection in executor to avoid blocking
            loop = asyncio.get_event_loop()

            def test_sync():
                try:
                    # Log connection attempt (without password)
                    safe_conn_str = conn_str.replace(f"PWD={connection_params['password']}", "PWD=***")
                    print(f"Attempting connection with: {safe_conn_str}")

                    with pyodbc.connect(conn_str, timeout=30) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        return True, f"SQL Server connection successful using {selected_driver}"

                except pyodbc.Error as e:
                    error_msg = str(e)
                    if "timeout" in error_msg.lower():
                        return False, f"Connection timeout: {error_msg}. Check if server {server_with_port} is accessible and SQL Server is running."
                    elif "login" in error_msg.lower():
                        return False, f"Authentication failed: {error_msg}. Check username and password."
                    elif "network" in error_msg.lower() or "tcp" in error_msg.lower():
                        return False, f"Network error: {error_msg}. Check if server {server_with_port} is reachable and port is open."
                    else:
                        return False, f"Connection failed: {error_msg}"

            result = await loop.run_in_executor(None, test_sync)
            return result

        else:
            return False, f"Unsupported connection type: {connection_type}. Supported types: postgresql, azure_sql, sql_server"

    except Exception as e:
        return False, f"Connection failed: {str(e)}"