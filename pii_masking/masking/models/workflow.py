from sqlalchemy import Column, String, Integer, ForeignKey, Text, JSON, DateTime
from sqlalchemy.orm import relationship
from enum import Enum
from ...common.base_model import BaseModel


class WorkflowStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Workflow(BaseModel):
    __tablename__ = "workflows"

    name = Column(String, nullable=False, index=True)
    description = Column(Text, nullable=True)
    source_connection_id = Column(Integer, ForeignKey("database_connections.id"), nullable=False)
    destination_connection_id = Column(Integer, ForeignKey("database_connections.id"), nullable=False)
    status = Column(String, default=WorkflowStatus.DRAFT.value)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Relationships
    user = relationship("User", back_populates="workflows")
    source_connection = relationship("DatabaseConnection", foreign_keys=[source_connection_id], back_populates="source_workflows")
    destination_connection = relationship("DatabaseConnection", foreign_keys=[destination_connection_id], back_populates="destination_workflows")
    table_mappings = relationship("TableMapping", back_populates="workflow", cascade="all, delete-orphan")
    executions = relationship("WorkflowExecution", back_populates="workflow", cascade="all, delete-orphan")


class WorkflowExecution(BaseModel):
    __tablename__ = "workflow_executions"

    workflow_id = Column(Integer, ForeignKey("workflows.id"), nullable=False)
    status = Column(String, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    records_processed = Column(Integer, default=0)
    execution_logs = Column(JSON, nullable=True, default=[])
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Relationships
    workflow = relationship("Workflow", back_populates="executions")
    user = relationship("User", back_populates="workflow_executions")