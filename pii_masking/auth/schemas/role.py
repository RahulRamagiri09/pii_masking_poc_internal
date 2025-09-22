from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class RoleBase(BaseModel):
    rolename: str


class RoleCreate(RoleBase):
    pass


class RoleUpdate(BaseModel):
    rolename: Optional[str] = None
    is_active: Optional[bool] = None


class RoleResponse(RoleBase):
    id: int
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool

    class Config:
        from_attributes = True