from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from ...core.database import get_db
from ...auth.routes.auth import get_current_user
from ...auth.schemas.user import UserResponse
from ..schemas.connection import (
    ConnectionCreate,
    ConnectionResponse,
    ConnectionUpdate,
    TestConnectionRequest,
    TestConnectionResponse
)
from ..crud.connection import (
    create_connection,
    get_connection,
    get_connections,
    update_connection,
    delete_connection,
    test_connection
)
from ...core.config import settings

router = APIRouter()


def check_permission(user: UserResponse, operation: str):
    """Check if user has permission for the operation"""
    role = user.role.rolename.lower()

    if role == "admin":
        return True  # Admin has all permissions

    permissions = {
        "data_engineer": ["create", "read", "update", "delete", "test"],
        "data_analyst": ["read", "test"],
        "viewer": ["read"]
    }

    allowed_operations = permissions.get(role, [])
    return operation in allowed_operations


@router.post("/", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_database_connection(
    connection: ConnectionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Create a new database connection"""
    if not check_permission(current_user, "create"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to create connections"
        )

    return await create_connection(
        db,
        connection,
        current_user.id,
        current_user.id
    )


@router.get("/", response_model=List[ConnectionResponse])
async def list_connections(
    skip: int = 0,
    limit: int = settings.DEFAULT_PAGE_SIZE,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """List all database connections"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view connections"
        )

    # Limit the maximum page size
    limit = min(limit, settings.MAX_PAGE_SIZE)

    # Admin sees all connections, others see only their own
    if current_user.role.rolename.lower() == "admin":
        return await get_connections(db, skip=skip, limit=limit)
    else:
        return await get_connections(db, user_id=current_user.id, skip=skip, limit=limit)


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_database_connection(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Get a specific database connection"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view connections"
        )

    connection = await get_connection(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )

    # Check ownership unless admin
    if current_user.role.rolename.lower() != "admin" and connection.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have access to this connection"
        )

    return connection


@router.put("/{connection_id}", response_model=ConnectionResponse)
async def update_database_connection(
    connection_id: int,
    connection_update: ConnectionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Update a database connection"""
    if not check_permission(current_user, "update"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update connections"
        )

    # Check if connection exists
    connection = await get_connection(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )

    # Check ownership unless admin
    if current_user.role.rolename.lower() != "admin" and connection.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to update this connection"
        )

    updated = await update_connection(db, connection_id, connection_update, current_user.id)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )

    return updated


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_database_connection(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Delete a database connection (soft delete)"""
    if not check_permission(current_user, "delete"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to delete connections"
        )

    # Check if connection exists
    connection = await get_connection(db, connection_id)
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )

    # Check ownership unless admin
    if current_user.role.rolename.lower() != "admin" and connection.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to delete this connection"
        )

    deleted = await delete_connection(db, connection_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found"
        )


@router.post("/test", response_model=TestConnectionResponse)
async def test_database_connection(
    test_request: TestConnectionRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Test a database connection and optionally update existing connection status"""
    if not check_permission(current_user, "test"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to test connections"
        )

    import time
    start = time.time()

    success, message = await test_connection({
        "connection_type": test_request.connection_type.value,
        "server": test_request.server,
        "database": test_request.database,
        "username": test_request.username,
        "password": test_request.password,
        "port": test_request.port
    })

    elapsed = (time.time() - start) * 1000  # Convert to milliseconds

    # If connection_id is provided, update the existing connection's status
    if test_request.connection_id:
        existing_connection = await get_connection(db, test_request.connection_id)
        if not existing_connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Connection not found"
            )

        # Check ownership unless admin
        if current_user.role.rolename.lower() != "admin" and existing_connection.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have access to this connection"
            )

        # Update connection status based on test result
        from ..models.connection import ConnectionStatus
        existing_connection.status = ConnectionStatus.ACTIVE.value if success else ConnectionStatus.ERROR.value
        existing_connection.test_connection_result = message
        existing_connection.updated_by = current_user.id

        await db.commit()
        await db.refresh(existing_connection)

    return TestConnectionResponse(
        success=success,
        message=message,
        connection_time_ms=elapsed if success else None
    )