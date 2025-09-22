from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from ..models.workflow import WorkflowStatus
from .mapping import TableMappingCreate, TableMappingResponse
from .connection import ConnectionResponse


class WorkflowBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None


class WorkflowCreate(WorkflowBase):
    source_connection_id: int
    destination_connection_id: int
    table_mappings: List[TableMappingCreate] = []


class WorkflowUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    source_connection_id: Optional[int] = None
    destination_connection_id: Optional[int] = None
    status: Optional[WorkflowStatus] = None
    table_mappings: Optional[List[TableMappingCreate]] = None


class WorkflowResponse(WorkflowBase):
    id: int
    source_connection_id: int
    destination_connection_id: int
    status: WorkflowStatus
    user_id: int
    table_mappings: List[TableMappingResponse] = []
    source_connection: Optional[ConnectionResponse] = None
    destination_connection: Optional[ConnectionResponse] = None
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool

    class Config:
        from_attributes = True


class WorkflowExecutionResponse(BaseModel):
    id: int
    workflow_id: int
    status: WorkflowStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    records_processed: int = 0
    execution_logs: List[str] = []
    user_id: int
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool

    class Config:
        from_attributes = True


class ExecuteWorkflowRequest(BaseModel):
    workflow_id: int


class ExecuteWorkflowResponse(BaseModel):
    execution_id: int
    message: str
    status: WorkflowStatus