from sqlalchemy import Column, Integer, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class BaseModel(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True, index=True)

    # Audit columns for tracking who and when
    created_by = Column(Integer, nullable=True, comment="User ID who created this record")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_by = Column(Integer, nullable=True, comment="User ID who last updated this record")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)

    # Soft delete flag
    is_active = Column(Boolean, default=True, nullable=False)