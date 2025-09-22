from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional, List
from ..models.role import Role
from ..schemas.role import RoleCreate, RoleUpdate


async def create_role(db: AsyncSession, role: RoleCreate, created_by: int = None) -> Role:
    db_role = Role(
        rolename=role.rolename,
        created_by=created_by
    )
    db.add(db_role)
    await db.commit()
    await db.refresh(db_role)
    return db_role


async def get_role(db: AsyncSession, role_id: int) -> Optional[Role]:
    result = await db.execute(
        select(Role).options(selectinload(Role.users)).where(Role.id == role_id)
    )
    return result.scalar_one_or_none()


async def get_role_by_name(db: AsyncSession, rolename: str) -> Optional[Role]:
    result = await db.execute(
        select(Role).where(Role.rolename == rolename)
    )
    return result.scalar_one_or_none()


async def get_roles(
    db: AsyncSession, skip: int, limit: int
) -> List[Role]:
    result = await db.execute(
        select(Role).options(selectinload(Role.users)).offset(skip).limit(limit)
    )
    return result.scalars().all()


async def update_role(
    db: AsyncSession, role_id: int, role_update: RoleUpdate, updated_by: int = None
) -> Optional[Role]:
    result = await db.execute(
        select(Role).where(Role.id == role_id)
    )
    db_role = result.scalar_one_or_none()

    if db_role:
        update_data = role_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_role, field, value)

        # Set audit fields
        if updated_by is not None:
            db_role.updated_by = updated_by

        await db.commit()
        await db.refresh(db_role)

    return db_role


async def delete_role(db: AsyncSession, role_id: int) -> bool:
    result = await db.execute(
        select(Role).where(Role.id == role_id)
    )
    db_role = result.scalar_one_or_none()

    if db_role:
        await db.delete(db_role)
        await db.commit()
        return True

    return False