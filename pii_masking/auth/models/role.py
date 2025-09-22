from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from ...common.base_model import BaseModel


class Role(BaseModel):
    __tablename__ = "roles"

    rolename = Column(String, unique=True, index=True, nullable=False)

    users = relationship("User", back_populates="role")

