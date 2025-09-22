from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional, List
from ..models.user import User
from ..schemas.user import UserCreate, UserUpdate
from ..security import get_password_hash, verify_password


async def create_user(db: AsyncSession, user: UserCreate, created_by: int = None) -> User:
    hashed_password = get_password_hash(user.password)
    db_user = User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password,
        role_id=user.role_id,
        created_by=created_by
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)

    # Reload user with role relationship to avoid lazy loading issues
    result = await db.execute(
        select(User).options(selectinload(User.role)).where(User.id == db_user.id)
    )
    return result.scalar_one()


async def get_user(db: AsyncSession, user_id: int) -> Optional[User]:
    result = await db.execute(
        select(User).options(selectinload(User.role)).where(User.id == user_id)
    )
    return result.scalar_one_or_none()


async def get_user_by_username(db: AsyncSession, username: str) -> Optional[User]:
    result = await db.execute(
        select(User).options(selectinload(User.role)).where(User.username == username)
    )
    return result.scalar_one_or_none()


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    result = await db.execute(
        select(User).options(selectinload(User.role)).where(User.email == email)
    )
    return result.scalar_one_or_none()


async def get_users(
    db: AsyncSession, skip: int, limit: int
) -> List[User]:
    result = await db.execute(
        select(User).options(selectinload(User.role)).offset(skip).limit(limit)
    )
    return result.scalars().all()


async def authenticate_user(
    db: AsyncSession, username: str, password: str
) -> Optional[User]:
    user = await get_user_by_username(db, username)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


async def update_user(
    db: AsyncSession, user_id: int, user_update: UserUpdate, updated_by: int = None
) -> Optional[User]:
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    db_user = result.scalar_one_or_none()

    if db_user:
        update_data = user_update.model_dump(exclude_unset=True)

        if "password" in update_data:
            update_data["hashed_password"] = get_password_hash(update_data.pop("password"))

        for field, value in update_data.items():
            setattr(db_user, field, value)

        # Set audit fields
        if updated_by is not None:
            db_user.updated_by = updated_by

        await db.commit()
        await db.refresh(db_user)

    return db_user


async def delete_user(db: AsyncSession, user_id: int) -> bool:
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    db_user = result.scalar_one_or_none()

    if db_user:
        await db.delete(db_user)
        await db.commit()
        return True

    return False