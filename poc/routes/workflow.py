from quart import Blueprint, request, jsonify, current_app
from services.workflow_service import WorkflowService
from models import Workflow, CreateWorkflowRequest, PII_ATTRIBUTES
import logging
import os

logger = logging.getLogger(__name__)
workflows_bp = Blueprint('workflows', __name__)

def get_workflow_service():
    """Get workflow service from app config"""
    app_config = current_app.config['APP_CONFIG']
    # The WorkflowService expects cosmos_client, not database attribute
    return WorkflowService(app_config.cosmos_client)

@workflows_bp.route('', methods=['GET'])
async def get_workflows():
    """Get all workflows"""
    try:
        workflow_service = get_workflow_service()
        workflows = await workflow_service.get_all_workflows()

        workflows_data = [workflow.dict() for workflow in workflows]

        return jsonify({
            "success": True,
            "data": workflows_data
        })
    except Exception as e:
        logger.error(f"Failed to get workflows: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@workflows_bp.route('', methods=['POST'])
async def create_workflow():
    """Create a new workflow"""
    try:
        data = await request.get_json()
        create_request = CreateWorkflowRequest(**data)

        workflow = Workflow(
            name=create_request.name,
            description=create_request.description,
            source_connection_id=create_request.source_connection_id,
            destination_connection_id=create_request.destination_connection_id,
            table_mappings=create_request.table_mappings
        )

        workflow_service = get_workflow_service()
        saved_workflow = await workflow_service.save_workflow(workflow)

        return jsonify({
            "success": True,
            "data": saved_workflow.dict(),
            "message": "Workflow created successfully"
        })

    except Exception as e:
        logger.error(f"Failed to create workflow: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@workflows_bp.route('/<workflow_id>', methods=['GET'])
async def get_workflow(workflow_id: str):
    """Get a specific workflow"""
    try:
        workflow_service = get_workflow_service()
        workflow = await workflow_service.get_workflow_by_id(workflow_id)

        if not workflow:
            return jsonify({
                "success": False,
                "error": "Workflow not found"
            }), 404

        return jsonify({
            "success": True,
            "data": workflow.dict()
        })

    except Exception as e:
        logger.error(f"Failed to get workflow: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@workflows_bp.route('/<workflow_id>', methods=['PUT'])
async def update_workflow(workflow_id: str):
    """Update a workflow"""
    try:
        data = await request.get_json()

        workflow_service = get_workflow_service()
        workflow = await workflow_service.get_workflow_by_id(workflow_id)

        if not workflow:
            return jsonify({
                "success": False,
                "error": "Workflow not found"
            }), 404

        # Update workflow fields
        if 'name' in data:
            workflow.name = data['name']
        if 'description' in data:
            workflow.description = data['description']
        if 'table_mappings' in data:
            workflow.table_mappings = data['table_mappings']

        saved_workflow = await workflow_service.save_workflow(workflow)

        return jsonify({
            "success": True,
            "data": saved_workflow.dict(),
            "message": "Workflow updated successfully"
        })

    except Exception as e:
        logger.error(f"Failed to update workflow: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@workflows_bp.route('/<workflow_id>', methods=['DELETE'])
async def delete_workflow(workflow_id: str):
    """Delete a workflow"""
    try:
        workflow_service = get_workflow_service()
        success = await workflow_service.delete_workflow(workflow_id)

        if not success:
            return jsonify({
                "success": False,
                "error": "Workflow not found"
            }), 404

        return jsonify({
            "success": True,
            "message": "Workflow deleted successfully"
        })

    except Exception as e:
        logger.error(f"Failed to delete workflow: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@workflows_bp.route('/<workflow_id>/executions', methods=['GET'])
async def get_workflow_executions(workflow_id: str):
    """Get execution history for a workflow"""
    try:
        workflow_service = get_workflow_service()
        executions = await workflow_service.get_workflow_executions(workflow_id)

        executions_data = [execution.dict() for execution in executions]

        return jsonify({
            "success": True,
            "data": executions_data
        })

    except Exception as e:
        logger.error(f"Failed to get workflow executions: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@workflows_bp.route('/pii-attributes', methods=['GET'])
async def get_pii_attributes():
    """Get list of available PII attributes for masking"""
    return jsonify({
        "success": True,
        "data": PII_ATTRIBUTES
    })
 