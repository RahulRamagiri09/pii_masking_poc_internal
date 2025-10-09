from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import logging

from ...core.database import get_db
from ...auth.routes.auth import get_current_user
from ...auth.schemas.user import UserResponse
from ..schemas.workflow import (
    WorkflowCreate,
    WorkflowResponse,
    WorkflowUpdate,
    WorkflowExecutionResponse,
    ExecuteWorkflowRequest,
    ExecuteWorkflowResponse,
    PIIAttributesResponse
)
from ..crud.workflow import (
    create_workflow,
    get_workflow,
    get_workflows,
    update_workflow,
    delete_workflow,
    get_workflow_executions,
    create_workflow_execution,
    update_workflow_execution
)
from ..services.masking_service import DataMaskingService
from ..models.mapping import PII_ATTRIBUTES
from ...core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


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


@router.post("", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
async def create_masking_workflow(
    workflow: WorkflowCreate,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Create a new masking workflow"""
    if not check_permission(current_user, "create"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to create workflows"
        )

    return await create_workflow(
        db,
        workflow,
        current_user.id,
        current_user.id
    )


@router.get("", response_model=List[WorkflowResponse])
async def list_workflows(
    skip: int = 0,
    limit: int = settings.DEFAULT_PAGE_SIZE,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """List all workflows"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view workflows"
        )

    # Limit the maximum page size
    limit = min(limit, settings.MAX_PAGE_SIZE)

    # Admin sees all workflows, others see only their own
    if current_user.role.rolename.lower() == "admin":
        return await get_workflows(db, skip=skip, limit=limit)
    else:
        return await get_workflows(db, user_id=current_user.id, skip=skip, limit=limit)


@router.get("/pii-attributes", response_model=PIIAttributesResponse)
async def get_pii_attributes(
    current_user: UserResponse = Depends(get_current_user)
):
    """Get list of available PII attributes for masking"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view PII attributes"
        )

    return PIIAttributesResponse(
        data=PII_ATTRIBUTES,
        success=True
    )


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_masking_workflow(
    workflow_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Get a specific workflow"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view workflows"
        )

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
            detail="You don't have access to this workflow"
        )

    return workflow


@router.put("/{workflow_id}", response_model=WorkflowResponse)
async def update_masking_workflow(
    workflow_id: int,
    workflow_update: WorkflowUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Update a workflow"""
    if not check_permission(current_user, "update"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update workflows"
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
            detail="You don't have permission to update this workflow"
        )

    updated = await update_workflow(db, workflow_id, workflow_update, current_user.id)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found"
        )

    return updated


@router.delete("/{workflow_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_masking_workflow(
    workflow_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Delete a workflow (soft delete)"""
    if not check_permission(current_user, "delete"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to delete workflows"
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
            detail="You don't have permission to delete this workflow"
        )

    deleted = await delete_workflow(db, workflow_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Workflow not found"
        )


async def _execute_workflow_background(
    workflow_id: int,
    execution_id: int,
    user_id: int
):
    """Background task to execute workflow"""
    # Create a new database session for the background task
    from ...core.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        masking_service = DataMaskingService()

        try:
            execution_result = await masking_service.execute_workflow(
                db=db,
                workflow_id=workflow_id,
                user_id=user_id,
                execution_id=execution_id
            )

            logger.info(
                f"Workflow execution completed in background: workflow_id={workflow_id}, "
                f"execution_id={execution_id}, records_processed={execution_result.records_processed}"
            )

        except Exception as e:
            logger.error(f"Background workflow execution failed: {e}", exc_info=True)
            # Exception is already handled in masking_service.execute_workflow
            # which updates the execution status to FAILED


@router.post("/{workflow_id}/execute", response_model=ExecuteWorkflowResponse)
async def execute_masking_workflow(
    workflow_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Execute a masking workflow in the background"""
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

    # Create execution record with RUNNING status
    from ..models.workflow import WorkflowStatus
    execution = await create_workflow_execution(db, workflow_id, current_user.id, WorkflowStatus.RUNNING)

    # Add the workflow execution task to background tasks
    background_tasks.add_task(
        _execute_workflow_background,
        workflow_id,
        execution.id,
        current_user.id
    )

    logger.info(
        f"Workflow execution started in background: workflow_id={workflow_id}, "
        f"execution_id={execution.id}"
    )

    # Return immediately with processing status
    return ExecuteWorkflowResponse(
        execution_id=execution.id,
        workflow_id=workflow_id,
        message=f"Workflow execution started in background. Use execution ID {execution.id} to check status.",
        status="processing"
    )


@router.get("/{workflow_id}/executions", response_model=List[WorkflowExecutionResponse])
async def get_workflow_execution_history(
    workflow_id: int,
    skip: int = 0,
    limit: int = settings.DEFAULT_PAGE_SIZE,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Get execution history for a workflow"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view execution history"
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
            detail="You don't have access to this workflow's execution history"
        )

    # Limit the maximum page size
    limit = min(limit, settings.MAX_PAGE_SIZE)

    return await get_workflow_executions(db, workflow_id, skip, limit)


@router.get("/{workflow_id}/executions/{execution_id}/status")
async def get_execution_status(
    workflow_id: int,
    execution_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserResponse = Depends(get_current_user)
):
    """Get detailed status of a workflow execution"""
    if not check_permission(current_user, "read"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to view execution status"
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
            detail="You don't have access to this workflow's execution status"
        )

    # Get execution from database
    from ..crud.workflow import get_workflow_execution_by_id
    execution = await get_workflow_execution_by_id(db, execution_id)

    if not execution:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution {execution_id} not found"
        )

    # Return execution status
    return {
        "execution_id": execution.id,
        "workflow_id": execution.workflow_id,
        "status": execution.status,
        "started_at": execution.started_at.isoformat() if execution.started_at else None,
        "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
        "records_processed": execution.records_processed,
        "error_message": execution.error_message,
        "execution_logs": execution.execution_logs or [],
        "user_id": execution.user_id
    }


