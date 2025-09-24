from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from ...core.database import get_db
from ..schemas.role import RoleCreate, RoleResponse, RoleUpdate
from ..crud.role import create_role, get_role, get_role_by_name, get_roles, update_role, delete_role
from .auth import get_current_user
from ..schemas.user import UserResponse
from ..dependencies import require_admin_role
from ...core.config import settings

router = APIRouter()


@router.post("", response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def register_role(
    role: RoleCreate,
    db: AsyncSession = Depends(get_db),
    current_admin: UserResponse = Depends(require_admin_role)
):
    """
    Create a new role. Only Admin users can create new roles.
    """
    # Check if role already exists
    existing_role = await get_role_by_name(db, role.rolename)
    if existing_role:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role with this name already exists"
        )

    return await create_role(db, role, created_by=current_admin.id)


@router.get("", response_model=List[RoleResponse])
async def read_roles(
    skip: int = 0,
    limit: int = settings.DEFAULT_PAGE_SIZE,
    db: AsyncSession = Depends(get_db)
):
    """
    Get all roles.

    Public access is allowed for initial setup (when no users exist).
    This allows the initial admin user creation process to work.
    """
    # Limit the maximum page size
    limit = min(limit, settings.MAX_PAGE_SIZE)
    return await get_roles(db, skip=skip, limit=limit)


@router.get("/{role_id}", response_model=RoleResponse)
async def read_role(
    role_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    role = await get_role(db, role_id)
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    return role


@router.put("/{role_id}", response_model=RoleResponse)
async def update_role_endpoint(
    role_id: int,
    role_update: RoleUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    role = await update_role(db, role_id, role_update)
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    return role


@router.delete("/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_role_endpoint(
    role_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    deleted = await delete_role(db, role_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )