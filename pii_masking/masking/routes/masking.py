from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from ...core.database import get_db
from ...auth.routes.auth import get_current_user
from ...auth.schemas.user import UserResponse
from ..schemas.mapping import (
    PiiAttributesResponse,
    MaskingPreviewRequest,
    MaskingPreviewResponse
)
from ..models.mapping import PII_ATTRIBUTES
from ..services.masking_service import DataMaskingService

router = APIRouter()


@router.get("/pii-attributes", response_model=PiiAttributesResponse)
async def get_pii_attributes(
    current_user: UserResponse = Depends(get_current_user)
):
    """Get all available PII attributes for masking"""
    return PiiAttributesResponse(attributes=PII_ATTRIBUTES)


@router.post("/preview", response_model=MaskingPreviewResponse)
async def preview_masking(
    preview_request: MaskingPreviewRequest,
    current_user: UserResponse = Depends(get_current_user)
):
    """Generate preview samples of masked data for a given PII attribute"""
    if preview_request.pii_attribute not in PII_ATTRIBUTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid PII attribute. Must be one of: {', '.join(PII_ATTRIBUTES)}"
        )

    masking_service = DataMaskingService()
    samples = masking_service.generate_sample_masked_data(
        preview_request.pii_attribute,
        preview_request.count,
        preview_request.sample_value
    )

    return MaskingPreviewResponse(
        pii_attribute=preview_request.pii_attribute,
        samples=samples
    )