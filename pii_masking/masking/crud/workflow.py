from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import Optional, List
from ..models.workflow import Workflow, WorkflowExecution, WorkflowStatus
from ..models.mapping import TableMapping, ColumnMapping
from ..schemas.workflow import WorkflowCreate, WorkflowUpdate
from datetime import datetime


async def create_workflow(
    db: AsyncSession,
    workflow: WorkflowCreate,
    user_id: int,
    created_by: int = None
) -> Workflow:
    """Create a new workflow with table and column mappings"""
    # Create workflow
    db_workflow = Workflow(
        name=workflow.name,
        description=workflow.description,
        source_connection_id=workflow.source_connection_id,
        destination_connection_id=workflow.destination_connection_id,
        user_id=user_id,
        created_by=created_by or user_id,
        status=WorkflowStatus.DRAFT.value
    )

    db.add(db_workflow)
    await db.flush()  # Get workflow ID without committing

    # Create table mappings and column mappings
    for table_mapping in workflow.table_mappings:
        db_table_mapping = TableMapping(
            workflow_id=db_workflow.id,
            source_table=table_mapping.source_table,
            destination_table=table_mapping.destination_table,
            created_by=created_by or user_id
        )
        db.add(db_table_mapping)
        await db.flush()

        # Create column mappings for this table
        for column_mapping in table_mapping.column_mappings:
            db_column_mapping = ColumnMapping(
                table_mapping_id=db_table_mapping.id,
                source_column=column_mapping.source_column,
                destination_column=column_mapping.destination_column,
                is_pii=column_mapping.is_pii,
                pii_attribute=column_mapping.pii_attribute,
                created_by=created_by or user_id
            )
            db.add(db_column_mapping)

    await db.commit()
    await db.refresh(db_workflow)

    # Load with all relationships
    result = await db.execute(
        select(Workflow)
        .options(
            selectinload(Workflow.user),
            selectinload(Workflow.source_connection),
            selectinload(Workflow.destination_connection),
            selectinload(Workflow.table_mappings).selectinload(TableMapping.column_mappings)
        )
        .where(Workflow.id == db_workflow.id)
    )
    return result.scalar_one()


async def get_workflow(db: AsyncSession, workflow_id: int) -> Optional[Workflow]:
    """Get a workflow by ID with all relationships"""
    result = await db.execute(
        select(Workflow)
        .options(
            selectinload(Workflow.user),
            selectinload(Workflow.source_connection),
            selectinload(Workflow.destination_connection),
            selectinload(Workflow.table_mappings).selectinload(TableMapping.column_mappings)
        )
        .where(Workflow.id == workflow_id)
    )
    return result.scalar_one_or_none()


async def get_workflows(
    db: AsyncSession,
    user_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100
) -> List[Workflow]:
    """Get all workflows, optionally filtered by user"""
    query = select(Workflow).options(
        selectinload(Workflow.user),
        selectinload(Workflow.source_connection),
        selectinload(Workflow.destination_connection),
        selectinload(Workflow.table_mappings).selectinload(TableMapping.column_mappings)
    )

    if user_id:
        query = query.where(Workflow.user_id == user_id)

    query = query.order_by(Workflow.id).offset(skip).limit(limit)
    result = await db.execute(query)
    return result.scalars().all()


async def update_workflow(
    db: AsyncSession,
    workflow_id: int,
    workflow_update: WorkflowUpdate,
    updated_by: int = None
) -> Optional[Workflow]:
    """Update a workflow"""
    result = await db.execute(
        select(Workflow).where(Workflow.id == workflow_id)
    )
    db_workflow = result.scalar_one_or_none()

    if not db_workflow:
        return None

    update_data = workflow_update.model_dump(exclude_unset=True)

    # Handle status enum
    if "status" in update_data:
        update_data["status"] = update_data["status"].value

    # Handle table mappings update
    if "table_mappings" in update_data:
        # Delete existing mappings
        result = await db.execute(
            select(TableMapping).where(TableMapping.workflow_id == workflow_id)
        )
        existing_mappings = result.scalars().all()
        for mapping in existing_mappings:
            await db.delete(mapping)

        # Add new mappings
        for table_mapping in update_data.pop("table_mappings"):
            db_table_mapping = TableMapping(
                workflow_id=workflow_id,
                source_table=table_mapping["source_table"],
                destination_table=table_mapping["destination_table"],
                created_by=updated_by
            )
            db.add(db_table_mapping)
            await db.flush()

            for column_mapping in table_mapping.get("column_mappings", []):
                db_column_mapping = ColumnMapping(
                    table_mapping_id=db_table_mapping.id,
                    source_column=column_mapping["source_column"],
                    destination_column=column_mapping["destination_column"],
                    is_pii=column_mapping.get("is_pii", False),
                    pii_attribute=column_mapping.get("pii_attribute"),
                    created_by=updated_by
                )
                db.add(db_column_mapping)

    # Update other fields
    for field, value in update_data.items():
        setattr(db_workflow, field, value)

    if updated_by is not None:
        db_workflow.updated_by = updated_by

    await db.commit()
    await db.refresh(db_workflow)

    # Load with relationships
    result = await db.execute(
        select(Workflow)
        .options(
            selectinload(Workflow.user),
            selectinload(Workflow.source_connection),
            selectinload(Workflow.destination_connection),
            selectinload(Workflow.table_mappings).selectinload(TableMapping.column_mappings)
        )
        .where(Workflow.id == workflow_id)
    )
    return result.scalar_one()


async def delete_workflow(db: AsyncSession, workflow_id: int) -> bool:
    """Delete a workflow (soft delete)"""
    result = await db.execute(
        select(Workflow).where(Workflow.id == workflow_id)
    )
    db_workflow = result.scalar_one_or_none()

    if db_workflow:
        db_workflow.is_active = False
        await db.commit()
        return True

    return False


async def create_workflow_execution(
    db: AsyncSession,
    workflow_id: int,
    user_id: int
) -> WorkflowExecution:
    """Create a new workflow execution"""
    execution = WorkflowExecution(
        workflow_id=workflow_id,
        status=WorkflowStatus.RUNNING.value,
        started_at=datetime.utcnow(),
        user_id=user_id,
        created_by=user_id
    )
    db.add(execution)
    await db.commit()
    await db.refresh(execution)
    return execution


async def update_workflow_execution(
    db: AsyncSession,
    execution_id: int,
    status: WorkflowStatus,
    error_message: Optional[str] = None,
    records_processed: Optional[int] = None,
    execution_logs: Optional[List[str]] = None
) -> Optional[WorkflowExecution]:
    """Update workflow execution status"""
    result = await db.execute(
        select(WorkflowExecution).where(WorkflowExecution.id == execution_id)
    )
    execution = result.scalar_one_or_none()

    if execution:
        execution.status = status.value
        if status in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED]:
            execution.completed_at = datetime.utcnow()
        if error_message:
            execution.error_message = error_message
        if records_processed is not None:
            execution.records_processed = records_processed
        if execution_logs:
            execution.execution_logs = execution_logs

        await db.commit()
        await db.refresh(execution)

    return execution


async def get_workflow_executions(
    db: AsyncSession,
    workflow_id: int,
    skip: int = 0,
    limit: int = 100
) -> List[WorkflowExecution]:
    """Get execution history for a workflow"""
    result = await db.execute(
        select(WorkflowExecution)
        .where(WorkflowExecution.workflow_id == workflow_id)
        .order_by(WorkflowExecution.started_at.desc())
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()