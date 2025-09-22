from fastapi import HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from .routes.auth import get_current_user
from .models.user import User


async def require_admin_role(current_user: User = Depends(get_current_user)) -> User:
    """
    Dependency that ensures the current user has Admin role.
    Raises 403 Forbidden if user doesn't have Admin role.
    """
    if not current_user.role or current_user.role.rolename != "Admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Admin users can perform this action"
        )
    return current_user 