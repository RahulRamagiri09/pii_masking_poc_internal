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
    """Execute a masking workflow"""
    if not check_permission(current_user, "execute"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to execute workflows"
        )

    # Check if workflow exists
    workflow = await get_workflow(db, workflow_id)
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found"
        )

    # Check ownership unless admin
    if current_user.role.rolename.lower() != "admin" and workflow.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to execute this workflow"
        )

    # Execute workflow
    masking_service = DataMaskingService()
    try:
        execution = await masking_service.execute_workflow(db, workflow_id, current_user.id)

        return MaskingExecuteResponse(
            data=MaskingExecuteData(
                execution_id=str(execution.id),
                started_at=execution.started_at or execution.created_at,
                status=execution.status
            ),
            message="workflow execution started",
            success=True
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute workflow: {str(e)}"
        )