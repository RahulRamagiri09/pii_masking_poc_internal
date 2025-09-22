from sqlalchemy import Column, String, Integer, JSON, Text, ForeignKey
from sqlalchemy.orm import relationship
from enum import Enum
from ...common.base_model import BaseModel


class ConnectionType(str, Enum):
    AZURE_SQL = "azure_sql"
    ORACLE = "oracle"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQL_SERVER = "sql_server"


class ConnectionStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


class DatabaseConnection(BaseModel):
    __tablename__ = "database_connections"

    name = Column(String, nullable=False, index=True)
    connection_type = Column(String, nullable=False)
    server = Column(String, nullable=False)
    database = Column(String, nullable=True)
    username = Column(String, nullable=False)
    password_encrypted = Column(Text, nullable=False)  # Encrypted password
    port = Column(Integer, nullable=True)
    additional_params = Column(JSON, nullable=True, default={})
    status = Column(String, default=ConnectionStatus.INACTIVE.value)
    test_connection_result = Column(Text, nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Relationships
    user = relationship("User", back_populates="database_connections")
    source_workflows = relationship("Workflow", foreign_keys="Workflow.source_connection_id", back_populates="source_connection")
    destination_workflows = relationship("Workflow", foreign_keys="Workflow.destination_connection_id", back_populates="destination_connection")