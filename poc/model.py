from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class ConnectionType(str, Enum):
    AZURE_SQL = "azure_sql"
    ORACLE = "oracle"


class ConnectionStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


class WorkflowStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DatabaseConnection(BaseModel):
    """Database connection model"""
    id: Optional[str] = None
    name: str
    connection_type: ConnectionType
    server: str
    username: str
    # password_key_vault_name will store the Key Vault secret name
    password_key_vault_name: str
    port: Optional[int] = None
    additional_params: Optional[Dict[str, Any]] = {}
    status: ConnectionStatus = ConnectionStatus.INACTIVE
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    test_connection_result: Optional[str] = None


class ColumnMapping(BaseModel):
    """Column mapping between source and destination"""
    source_column: str
    destination_column: str
    is_pii: bool = False
    pii_attribute: Optional[str] = None  # From the predefined PII attributes list


class TableMapping(BaseModel):
    """Table mapping configuration"""
    source_table: str
    destination_table: str
    column_mappings: List[ColumnMapping]


class Workflow(BaseModel):
    """Workflow model for masking and copying data"""
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    source_connection_id: str
    destination_connection_id: str
    table_mappings: List[TableMapping]
    status: WorkflowStatus = WorkflowStatus.DRAFT
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None


class WorkflowExecution(BaseModel):
    """Workflow execution history"""
    id: Optional[str] = None
    workflow_id: str
    status: WorkflowStatus
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    records_processed: Optional[int] = 0
    logs: List[str] = []


class TestConnectionRequest(BaseModel):
    """Request model for testing database connection"""
    connection_type: ConnectionType
    server: str
    database: str
    username: str
    password: str
    port: Optional[int] = None
    additional_params: Optional[Dict[str, Any]] = {}


class CreateWorkflowRequest(BaseModel):
    """Request model for creating a new workflow"""
    name: str
    description: Optional[str] = None
    source_connection_id: str
    destination_connection_id: str
    table_mappings: List[TableMapping]


# Predefined PII attributes for masking
PII_ATTRIBUTES = [
    "address", "city", "city_prefix", "city_suffix", "company", "company_email",
    "company_suffix", "country", "country_calling_code", "country_code",
    "date_of_birth", "email", "first_name", "last_name", "name", "passport_dob",
    "passport_doc", "passport_full", "passport_gender", "passport_number",
    "passport_owner", "phone_number", "postalcode", "postcode", "profile",
    "secondary_address", "simple_profile", "ssn", "state", "state_abbr",
    "street_address", "street_name", "street_suffix", "zipcode",
    "zipcode_in_state", "zipcode_plus4"
]
