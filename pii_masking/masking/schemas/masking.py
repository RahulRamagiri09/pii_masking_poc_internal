from pydantic import BaseModel
from datetime import datetime


class MaskingExecuteData(BaseModel):
    execution_id: str
    started_at: datetime
    status: str


class MaskingExecuteResponse(BaseModel):
    data: MaskingExecuteData
    message: str
    success: bool