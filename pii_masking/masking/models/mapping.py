from sqlalchemy import Column, String, Integer, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from ...common.base_model import BaseModel


class TableMapping(BaseModel):
    __tablename__ = "table_mappings"

    workflow_id = Column(Integer, ForeignKey("workflows.id"), nullable=False)
    source_table = Column(String(255), nullable=False)
    destination_table = Column(String(255), nullable=False)

    # Relationships
    workflow = relationship("Workflow", back_populates="table_mappings")
    column_mappings = relationship("ColumnMapping", back_populates="table_mapping", cascade="all, delete-orphan")


class ColumnMapping(BaseModel):
    __tablename__ = "column_mappings"

    table_mapping_id = Column(Integer, ForeignKey("table_mappings.id"), nullable=False)
    source_column = Column(String(255), nullable=False)
    destination_column = Column(String(255), nullable=False)
    is_pii = Column(Boolean, default=False)
    pii_attribute = Column(String(100), nullable=True)  # From predefined PII attributes

    # Relationships
    table_mapping = relationship("TableMapping", back_populates="column_mappings")


# Predefined PII attributes for masking
PII_ATTRIBUTES = [
    "address", "city", "city_prefix", "city_suffix", "company", "company_email",
    "company_suffix", "country", "country_calling_code", "country_code",
    "date_of_birth", "email", "first_name", "last_name", "name", "passport_dob",
    "passport_full", "passport_gender", "passport_number", "passport_owner",
    "phone_number", "postalcode", "postcode", "profile", "secondary_address",
    "simple_profile", "ssn", "state", "state_abbr", "street_address",
    "street_name", "street_suffix", "zipcode", "zipcode_in_state", "zipcode_plus4"
]