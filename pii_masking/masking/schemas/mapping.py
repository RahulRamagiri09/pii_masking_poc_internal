from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class ColumnMappingBase(BaseModel):
    source_column: str = Field(..., min_length=1)
    destination_column: str = Field(..., min_length=1)
    is_pii: bool = False
    pii_attribute: Optional[str] = None


class ColumnMappingCreate(ColumnMappingBase):
    pass


class ColumnMappingUpdate(BaseModel):
    source_column: Optional[str] = Field(None, min_length=1)
    destination_column: Optional[str] = Field(None, min_length=1)
    is_pii: Optional[bool] = None
    pii_attribute: Optional[str] = None


class ColumnMappingResponse(ColumnMappingBase):
    id: int
    table_mapping_id: int
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool

    class Config:
        from_attributes = True


class ColumnMappingSimpleResponse(ColumnMappingBase):
    """Simplified column mapping response without audit fields"""

    class Config:
        from_attributes = True


class TableMappingBase(BaseModel):
    source_table: str = Field(..., min_length=1)
    destination_table: str = Field(..., min_length=1)


class TableMappingCreate(TableMappingBase):
    column_mappings: List[ColumnMappingCreate] = []


class TableMappingUpdate(BaseModel):
    source_table: Optional[str] = Field(None, min_length=1)
    destination_table: Optional[str] = Field(None, min_length=1)
    column_mappings: Optional[List[ColumnMappingCreate]] = None


class TableMappingResponse(TableMappingBase):
    id: int
    workflow_id: int
    column_mappings: List[ColumnMappingSimpleResponse] = []
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: Optional[datetime] = None
    is_active: bool

    class Config:
        from_attributes = True


class TableMappingSimpleResponse(TableMappingBase):
    """Simplified table mapping response without audit fields"""
    id: int
    workflow_id: int
    column_mappings: List[ColumnMappingSimpleResponse] = []

    class Config:
        from_attributes = True


class PiiAttributesResponse(BaseModel):
    attributes: List[str]


class MaskingPreviewRequest(BaseModel):
    pii_attribute: str
    sample_value: str = "sample"
    count: int = Field(5, ge=1, le=10)


class MaskingPreviewResponse(BaseModel):
    pii_attribute: str
    samples: List[str]