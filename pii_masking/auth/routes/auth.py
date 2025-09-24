from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.database import get_db
from ..schemas.user import UserLogin, Token, UserResponse, LoginResponse, UserInfo
from ..crud.user import authenticate_user, get_user_by_username
from ..security import create_access_token, verify_token
from ...core.config import settings

router = APIRouter()
security = HTTPBearer()


def get_user_permissions(role_name: str) -> list[str]:
    """Get permissions based on user role"""
    permissions_map = {
        "admin": [
            "create_roles", "read_roles", "update_roles", "delete_roles",
            "create_users", "read_users", "update_users", "delete_users",
            "create_connections", "read_connections", "update_connections", "delete_connections", "test_connections",
            "create_workflows", "read_workflows", "update_workflows", "delete_workflows", "execute_workflows"
        ],
        "data_engineer": [
            "read_roles", "read_users",
            "create_connections", "read_connections", "update_connections", "delete_connections", "test_connections",
            "create_workflows", "read_workflows", "update_workflows", "delete_workflows", "execute_workflows"
        ],
        "data_analyst": [
            "read_roles", "read_users",
            "read_connections", "test_connections",
            "read_workflows", "execute_workflows"
        ],
        "viewer": [
            "read_roles", "read_users",
            "read_connections",
            "read_workflows"
        ]
    }
    return permissions_map.get(role_name.lower(), [])


@router.post("/login", response_model=LoginResponse)
async def login_user(
    user_credentials: UserLogin,
    db: AsyncSession = Depends(get_db)
):
    user = await authenticate_user(db, user_credentials.username, user_credentials.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        subject=user.username, expires_delta=access_token_expires
    )

    # Get user permissions based on role
    role_name = user.role.rolename if user.role else "viewer"
    permissions = get_user_permissions(role_name)

    # Create user info
    user_info = UserInfo(
        id=user.id,
        username=user.username,
        email=user.email,
        role=role_name,
        permissions=permissions
    )

    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        user=user_info
    )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
) -> UserResponse:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    username = verify_token(credentials.credentials)
    if username is None:
        raise credentials_exception

    user = await get_user_by_username(db, username)
    if user is None:
        raise credentials_exception

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )

    return user