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
from ..schemas.masking import (
    MaskingExecuteResponse,
    MaskingExecuteData
)
from ..models.mapping import PII_ATTRIBUTES
from ..services.masking_service import DataMaskingService
from ..crud.workflow import get_workflow

router = APIRouter()


def check_permission(user: UserResponse, operation: str):
    """Check if user has permission for the operation"""
    role = user.role.rolename.lower()

    if role == "admin":
        return True  # Admin has all permissions

    permissions = {
        "data_engineer": ["create", "read", "update", "delete", "execute"],
        "data_analyst": ["read", "execute"],
        "viewer": ["read"]
    }

    allowed_operations = permissions.get(role, [])
    return operation in allowed_operations


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


@router.post("/execute/{workflow_id}", response_model=MaskingExecuteResponse)
async def execute_masking(
    workflow_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    DEPRECATED: This endpoint executes workflows synchronously and can cause timeouts.

    Please use the async endpoint instead:
    POST /api/workflows/{workflow_id}/execute

    This endpoint will be removed in a future version.
    """
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail={
            "error": "This endpoint is deprecated and has been removed",
            "reason": "Synchronous execution causes timeouts for large datasets",
            "solution": "Use the async Celery-based endpoint instead",
            "correct_endpoint": f"/api/workflows/{workflow_id}/execute",
            "correct_method": "POST",
            "migration_guide": {
                "step_1": f"Change your request URL to: POST /api/workflows/{workflow_id}/execute",
                "step_2": "The response will include a task_id and execution_id",
                "step_3": f"Poll GET /api/workflows/{workflow_id}/executions/{{execution_id}}/status for progress",
                "step_4": "Check for status: queued -> running -> completed/failed"
            },
            "benefits": [
                "No timeout issues - returns immediately",
                "Background processing with Celery",
                "Real-time progress tracking",
                "Proper error handling and retries"
            ]
        }
    )