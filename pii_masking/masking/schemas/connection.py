from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from ..models.connection import ConnectionType, ConnectionStatus


class ConnectionBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    connection_type: ConnectionType
    server: str = Field(..., min_length=1)
    database: Optional[str] = None
    username: str = Field(..., min_length=1)
    port: Optional[int] = Field(None, ge=1, le=65535)
    additional_params: Optional[Dict[str, Any]] = {}


class ConnectionCreate(ConnectionBase):
    password: str = Field(..., min_length=1)


class ConnectionUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    connection_type: Optional[ConnectionType] = None
    server: Optional[str] = Field(None, min_length=1)
    database: Optional[str] = None
    username: Optional[str] = Field(None, min_length=1)
    password: Optional[str] = Field(None, min_length=1)
    port: Optional[int] = Field(None, ge=1, le=65535)
    additional_params: Optional[Dict[str, Any]] = None
    status: Optional[ConnectionStatus] = None


class ConnectionResponse(ConnectionBase):
    id: int
    status: ConnectionStatus
    test_connection_result: Optional[str] = None
    user_id: int
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool

    class Config:
        from_attributes = True


class ConnectionSimpleResponse(ConnectionBase):
    """Simplified connection response without audit fields"""
    id: int
    status: ConnectionStatus
    test_connection_result: Optional[str] = None
    user_id: int

    class Config:
        from_attributes = True


class TestConnectionRequest(BaseModel):
    connection_type: ConnectionType
    server: str
    database: Optional[str] = None
    username: str
    password: str
    port: Optional[int] = None
    additional_params: Optional[Dict[str, Any]] = {}
    connection_id: Optional[int] = None  # If provided, update this existing connection's status


class TestConnectionResponse(BaseModel):
    success: bool
    message: str
    connection_time_ms: Optional[float] = None


class TablesResponse(BaseModel):
    data: List[str]
    success: bool


class ColumnInfo(BaseModel):
    name: str
    data_type: str
    is_nullable: bool
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None


class ColumnsResponse(BaseModel):
    data: List[ColumnInfo]
    success: bool