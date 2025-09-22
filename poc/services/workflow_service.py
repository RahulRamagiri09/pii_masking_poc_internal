from azure.cosmos import exceptions as cosmos_exceptions
from typing import List, Dict, Any, Optional
from models import Workflow, WorkflowExecution, WorkflowStatus
import json
import uuid
from datetime import datetime
import logging
import os

logger = logging.getLogger(__name__)

# Development mode local storage
DEV_MODE = os.getenv('DEVELOPMENT_MODE', 'False').lower() == 'true'
DEV_WORKFLOWS = []  # In-memory storage for workflows in dev mode
DEV_EXECUTIONS = []  # In-memory storage for workflow executions in dev mode

class WorkflowService:
    """Service for managing workflows"""

    def __init__(self, cosmos_client):
        self.cosmos_client = cosmos_client
        self.workflows_container = self._get_or_create_container('workflows')
        self.executions_container = self._get_or_create_container('workflow_executions')

    def _get_or_create_container(self, container_name: str):
        """Get or create a Cosmos DB container"""
        if self.cosmos_client is None:
            logger.warning(f"No Cosmos DB client available - returning None for container {container_name}")
            return None

        try:
            container = self.cosmos_client.get_container_client(container_name)
            # Test if container exists
            container.read()
            return container
        except cosmos_exceptions.CosmosResourceNotFoundError:
            # Create container if it doesn't exist
            return self.cosmos_client.create_container_if_not_exists(
                id=container_name,
                partition_key={'paths': ['/id'], 'kind': 'Hash'}
            )

    async def save_workflow(self, workflow: Workflow) -> Workflow:
        """Save workflow to Cosmos DB"""
        try:
            # Generate ID if not provided
            if not workflow.id:
                workflow.id = str(uuid.uuid4())

            workflow.updated_at = datetime.utcnow()

            if DEV_MODE:
                # In development mode, save to in-memory list
                # Check if workflow already exists
                for i, wf in enumerate(DEV_WORKFLOWS):
                    if wf.id == workflow.id:
                        # Update existing workflow
                        DEV_WORKFLOWS[i] = workflow
                        logger.info(f"DEVELOPMENT MODE: Workflow {workflow.name} updated in memory")
                        return workflow

                # Add new workflow
                DEV_WORKFLOWS.append(workflow)
                logger.info(f"DEVELOPMENT MODE: Workflow {workflow.name} saved to memory")
                return workflow
            else:
                # In production, save to Cosmos DB
                # Convert to dict for Cosmos DB
                workflow_dict = workflow.dict()

                # Save to Cosmos DB
                self.workflows_container.upsert_item(workflow_dict)

                logger.info(f"Workflow {workflow.name} saved successfully")
                return workflow

        except Exception as e:
            logger.error(f"Failed to save workflow: {e}")
            raise

    async def get_all_workflows(self) -> List[Workflow]:
        """Retrieve all workflows"""
        try:
            if DEV_MODE:
                # In development mode, return from memory
                logger.info("DEVELOPMENT MODE: Returning workflows from local storage")
                return DEV_WORKFLOWS
            else:
                # In production, use Cosmos DB
                if self.workflows_container is None:
                    logger.warning("No Cosmos DB connection - returning empty list")
                    return []

                items = list(self.workflows_container.read_all_items())
                workflows = [Workflow(**item) for item in items]
                return workflows
        except Exception as e:
            logger.error(f"Failed to retrieve workflows: {e}")
            raise

    async def get_workflow_by_id(self, workflow_id: str) -> Optional[Workflow]:
        """Retrieve a specific workflow by ID"""
        try:
            if DEV_MODE:
                # In development mode, search in memory
                for wf in DEV_WORKFLOWS:
                    if wf.id == workflow_id:
                        return wf
                return None
            else:
                # In production, use Cosmos DB
                if self.workflows_container is None:
                    logger.warning(f"No Cosmos DB connection - cannot get workflow {workflow_id}")
                    return None

                item = self.workflows_container.read_item(
                    item=workflow_id,
                    partition_key=workflow_id
                )
                return Workflow(**item)
        except cosmos_exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve workflow {workflow_id}: {e}")
            raise

    async def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow"""
        try:
            if DEV_MODE:
                # In development mode, remove from memory
                for i, wf in enumerate(DEV_WORKFLOWS):
                    if wf.id == workflow_id:
                        DEV_WORKFLOWS.pop(i)
                        logger.info(f"DEVELOPMENT MODE: Workflow {workflow_id} deleted from memory")
                        return True
                logger.warning(f"Workflow {workflow_id} not found in memory")
                return False
            else:
                # In production, delete from Cosmos DB
                if self.workflows_container is None:
                    logger.warning(f"No Cosmos DB connection - cannot delete workflow {workflow_id}")
                    return False

                self.workflows_container.delete_item(
                    item=workflow_id,
                    partition_key=workflow_id
                )
                logger.info(f"Workflow {workflow_id} deleted successfully")
                return True
        except cosmos_exceptions.CosmosResourceNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Failed to delete workflow {workflow_id}: {e}")
            raise

    async def update_workflow_status(self, workflow_id: str, status: WorkflowStatus) -> bool:
        """Update workflow status"""
        try:
            workflow = await self.get_workflow_by_id(workflow_id)
            if not workflow:
                return False

            workflow.status = status
            workflow.updated_at = datetime.utcnow()

            await self.save_workflow(workflow)
            return True

        except Exception as e:
            logger.error(f"Failed to update workflow status: {e}")
            raise

    async def create_execution(self, workflow_id: str) -> WorkflowExecution:
        """Create a new workflow execution record"""
        try:
            execution = WorkflowExecution(
                id=str(uuid.uuid4()),
                workflow_id=workflow_id,
                status=WorkflowStatus.RUNNING
            )

            if DEV_MODE:
                # In development mode, store in memory
                DEV_EXECUTIONS.append(execution)
                logger.info(f"DEVELOPMENT MODE: Workflow execution {execution.id} created in memory for workflow {workflow_id}")
            else:
                # In production, use Cosmos DB
                if self.executions_container is None:
                    logger.warning(f"No Cosmos DB connection - cannot create execution for workflow {workflow_id}")
                    # Still return the execution object for development mode
                else:
                    execution_dict = execution.dict()
                    self.executions_container.upsert_item(execution_dict)
                    logger.info(f"Workflow execution {execution.id} created for workflow {workflow_id}")

            return execution

        except Exception as e:
            logger.error(f"Failed to create workflow execution: {e}")
            raise

    async def update_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Update workflow execution"""
        try:
            if DEV_MODE:
                # In development mode, update in memory
                for i, exec_item in enumerate(DEV_EXECUTIONS):
                    if exec_item.id == execution.id:
                        DEV_EXECUTIONS[i] = execution
                        logger.info(f"DEVELOPMENT MODE: Workflow execution {execution.id} updated in memory")
                        return execution

                # If not found, add it
                DEV_EXECUTIONS.append(execution)
                logger.info(f"DEVELOPMENT MODE: Workflow execution {execution.id} added to memory")
            else:
                # In production, use Cosmos DB
                if self.executions_container is None:
                    logger.warning(f"No Cosmos DB connection - cannot update execution {execution.id}")
                else:
                    execution_dict = execution.dict()
                    self.executions_container.upsert_item(execution_dict)
                    logger.info(f"Workflow execution {execution.id} updated")

            return execution

        except Exception as e:
            logger.error(f"Failed to update workflow execution: {e}")
            raise

    async def get_workflow_executions(self, workflow_id: str) -> List[WorkflowExecution]:
        """Get execution history for a workflow"""
        try:
            if DEV_MODE:
                # In development mode, filter from memory
                executions = [exec_item for exec_item in DEV_EXECUTIONS if exec_item.workflow_id == workflow_id]
                # Sort by started_at in descending order
                executions.sort(key=lambda x: x.started_at if x.started_at else datetime.min, reverse=True)
                logger.info(f"DEVELOPMENT MODE: Retrieved {len(executions)} executions for workflow {workflow_id}")
                return executions
            else:
                # In production, use Cosmos DB
                if self.executions_container is None:
                    logger.warning(f"No Cosmos DB connection - cannot get executions for workflow {workflow_id}")
                    return []

                query = "SELECT * FROM c WHERE c.workflow_id = @workflow_id ORDER BY c.started_at DESC"
                parameters = [{"name": "@workflow_id", "value": workflow_id}]

                items = list(self.executions_container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True
                ))

                executions = [WorkflowExecution(**item) for item in items]
                return executions

        except Exception as e:
            logger.error(f"Failed to get workflow executions: {e}")
            raise

    async def get_execution_by_id(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Get a specific workflow execution"""
        try:
            if DEV_MODE:
                # In development mode, search in memory
                for exec_item in DEV_EXECUTIONS:
                    if exec_item.id == execution_id:
                        return exec_item
                return None
            else:
                # In production, use Cosmos DB
                if self.executions_container is None:
                    logger.warning(f"No Cosmos DB connection - cannot get execution {execution_id}")
                    return None

                item = self.executions_container.read_item(
                    item=execution_id,
                    partition_key=execution_id
                )
                return WorkflowExecution(**item)
        except cosmos_exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve execution {execution_id}: {e}")
            raise
