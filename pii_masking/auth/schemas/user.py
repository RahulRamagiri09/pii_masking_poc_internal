from pydantic import BaseModel, EmailStr
from datetime import datetime
from typing import Optional
from .role import RoleResponse


class UserBase(BaseModel):
    username: str
    email: EmailStr


class UserCreate(UserBase):
    password: str
    role_id: int


class UserLogin(BaseModel):
    username: str
    password: str


class UserUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    role_id: Optional[int] = None
    is_active: Optional[bool] = None


class UserResponse(UserBase):
    id: int
    role_id: int
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool
    role: Optional[RoleResponse] = None

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None