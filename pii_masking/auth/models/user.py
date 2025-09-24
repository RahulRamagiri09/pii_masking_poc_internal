from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from ...common.base_model import BaseModel


class User(BaseModel):
    __tablename__ = "users"

    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    role_id = Column(ForeignKey("roles.id"), nullable=False)

    role = relationship("Role", back_populates="users")

    # Masking relationships
    database_connections = relationship("DatabaseConnection", back_populates="user")
    workflows = relationship("Workflow", back_populates="user")
    workflow_executions = relationship("WorkflowExecution", back_populates="user")