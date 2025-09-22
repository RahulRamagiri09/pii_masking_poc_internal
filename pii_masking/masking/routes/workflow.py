from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from ...core.database import get_db
from ...auth.routes.auth import get_current_user
from ...auth.schemas.user import UserResponse
from ..schemas.workflow import (
    WorkflowCreate,
    WorkflowResponse,
    WorkflowUpdate,
    WorkflowExecutionResponse,
    ExecuteWorkflowRequest,
    ExecuteWorkflowResponse
)
from ..crud.workflow import (
    create_workflow,
    get_workflow,
    get_workflows,
    update_workflow,
    delete_workflow,
    get_workflow_executions
)
from ..services.masking_service import DataMaskingService
from ...core.config import settings

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


@router.post("/", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
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


@router.get("/", response_model=List[WorkflowResponse])
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


@router.post("/{workflow_id}/execute", response_model=ExecuteWorkflowResponse)
async def execute_masking_workflow(
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
    execution = await masking_service.execute_workflow(db, workflow_id, current_user.id)

    return ExecuteWorkflowResponse(
        execution_id=execution.id,
        message=f"Workflow execution {'completed' if execution.status == 'completed' else 'started'}",
        status=execution.status
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