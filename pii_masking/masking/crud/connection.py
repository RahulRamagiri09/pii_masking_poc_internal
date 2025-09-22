from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional, List
from cryptography.fernet import Fernet
import os
from ..models.connection import DatabaseConnection, ConnectionStatus
from ..schemas.connection import ConnectionCreate, ConnectionUpdate

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
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", Fernet.generate_key().decode())
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

    query = query.offset(skip).limit(limit)
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

            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={connection_params['server']};"
                f"DATABASE={connection_params.get('database', 'master')};"
                f"UID={connection_params['username']};"
                f"PWD={connection_params['password']}"
            )
            if connection_params.get('port'):
                conn_str = conn_str.replace(
                    f"SERVER={connection_params['server']};",
                    f"SERVER={connection_params['server']},{connection_params['port']};"
                )

            # Test connection in executor to avoid blocking
            loop = asyncio.get_event_loop()

            def test_sync():
                with pyodbc.connect(conn_str, timeout=5) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    return True, "SQL Server connection successful"

            result = await loop.run_in_executor(None, test_sync)
            return result

        else:
            return False, f"Unsupported connection type: {connection_type}. Supported types: postgresql, azure_sql, sql_server"

    except Exception as e:
        return False, f"Connection failed: {str(e)}"