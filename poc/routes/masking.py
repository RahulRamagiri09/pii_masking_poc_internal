from quart import Blueprint, request, jsonify, current_app
from services.masking_service import DataMaskingService
from services.database_service import DatabaseService
from services.workflow_service import WorkflowService
import logging
import asyncio
import os

logger = logging.getLogger(__name__)
masking_bp = Blueprint('masking', __name__)

def get_masking_service():
    """Get masking service from app config"""
    app_config = current_app.config['APP_CONFIG']
    db_service = DatabaseService(app_config.cosmos_client, app_config.keyvault_client)
    workflow_service = WorkflowService(app_config.cosmos_client)
    return DataMaskingService(db_service, workflow_service)

@masking_bp.route('/execute/<workflow_id>', methods=['POST'])
async def execute_workflow(workflow_id: str):
    """Execute a masking workflow"""
    try:
        masking_service = get_masking_service()

        # Start workflow execution in background
        execution = await masking_service.execute_workflow(workflow_id)

        return jsonify({
            "success": True,
            "data": {
                "execution_id": execution.id,
                "status": execution.status,
                "started_at": execution.started_at.isoformat()
            },
            "message": "Workflow execution started"
        })

    except Exception as e:
        logger.error(f"Failed to execute workflow: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@masking_bp.route('/execution/<execution_id>/status', methods=['GET'])
async def get_execution_status(execution_id: str):
    """Get the status of a workflow execution"""
    try:
        masking_service = get_masking_service()
        execution = await masking_service.workflow_service.get_execution_by_id(execution_id)

        if not execution:
            return jsonify({
                "success": False,
                "error": "Execution not found"
            }), 404

        return jsonify({
            "success": True,
            "data": execution.dict()
        })

    except Exception as e:
        logger.error(f"Failed to get execution status: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@masking_bp.route('/sample-data', methods=['POST'])
async def generate_sample_data():
    """Generate sample masked data for preview"""
    try:
        data = await request.get_json()
        pii_attribute = data.get('pii_attribute')
        count = data.get('count', 5)

        if not pii_attribute:
            return jsonify({
                "success": False,
                "error": "pii_attribute is required"
            }), 400

        masking_service = get_masking_service()
        samples = masking_service.generate_sample_masked_data(pii_attribute, count)

        return jsonify({
            "success": True,
            "data": {
                "pii_attribute": pii_attribute,
                "samples": samples
            }
        })

    except Exception as e:
        logger.error(f"Failed to generate sample data: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@masking_bp.route('/validate-workflow', methods=['POST'])
async def validate_workflow():
    """Validate a workflow configuration before execution"""
    try:
        data = await request.get_json()
        workflow_id = data.get('workflow_id')

        if not workflow_id:
            return jsonify({
                "success": False,
                "error": "workflow_id is required"
            }), 400

        masking_service = get_masking_service()

        # Get workflow
        workflow = await masking_service.workflow_service.get_workflow_by_id(workflow_id)
        if not workflow:
            return jsonify({
                "success": False,
                "error": "Workflow not found"
            }), 404

        # Validate connections
        source_conn = await masking_service.database_service.get_connection_by_id(
            workflow.source_connection_id
        )
        dest_conn = await masking_service.database_service.get_connection_by_id(
            workflow.destination_connection_id
        )

        validation_results = {
            "workflow_valid": True,
            "errors": [],
            "warnings": []
        }

        if not source_conn:
            validation_results["errors"].append("Source connection not found")
        if not dest_conn:
            validation_results["errors"].append("Destination connection not found")

        # Validate table mappings
        for table_mapping in workflow.table_mappings:
            pii_columns = [col for col in table_mapping.column_mappings if col.is_pii]
            if pii_columns:
                unmapped_pii = [col for col in pii_columns if not col.pii_attribute]
                if unmapped_pii:
                    validation_results["warnings"].append(
                        f"Table {table_mapping.source_table} has PII columns without attribute mapping"
                    )

        if validation_results["errors"]:
            validation_results["workflow_valid"] = False

        return jsonify({
            "success": True,
            "data": validation_results
        })

    except Exception as e:
        logger.error(f"Failed to validate workflow: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
 