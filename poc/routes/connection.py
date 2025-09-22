from quart import Blueprint, request, jsonify, current_app
from services.database_service import DatabaseService
from models import DatabaseConnection, TestConnectionRequest, ConnectionStatus
import uuid
import logging

logger = logging.getLogger(__name__)
connections_bp = Blueprint('connections', __name__)

def get_database_service():
    """Get database service from app config"""
    app_config = current_app.config['APP_CONFIG']
    # The DatabaseService expects cosmos_client, not database attribute
    return DatabaseService(app_config.cosmos_client, app_config.keyvault_client)

@connections_bp.route('', methods=['GET'])
async def get_connections():
    """Get all database connections"""
    try:
        db_service = get_database_service()
        connections = await db_service.get_all_connections()

        # Remove sensitive information from response
        connection_list = []
        for conn in connections:
            conn_dict = conn.dict()
            # Don't expose Key Vault secret names in the response
            conn_dict.pop('password_key_vault_name', None)
            connection_list.append(conn_dict)

        return jsonify({
            "success": True,
            "data": connection_list
        })
    except Exception as e:
        logger.error(f"Failed to get connections: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@connections_bp.route('', methods=['POST'])
async def create_connection():
    """Create a new database connection"""
    try:
        data = await request.get_json()

        # Validate required fields
        required_fields = ['name', 'connection_type', 'server', 'database', 'username', 'password']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "success": False,
                    "error": f"Missing required field: {field}"
                }), 400

        # Extract password and generate Key Vault secret name
        password = data.pop('password')
        password_key_name = f"db-password-{str(uuid.uuid4())}"

        # Create connection object
        connection = DatabaseConnection(
            name=data['name'],
            connection_type=data['connection_type'],
            server=data['server'],
            database=data['database'],
            username=data['username'],
            password_key_vault_name=password_key_name,
            port=data.get('port'),
            additional_params=data.get('additional_params', {})
        )

        db_service = get_database_service()

        # Save password to Key Vault
        await db_service.save_password_to_keyvault(password_key_name, password)

        # Save connection with INACTIVE status (default from model)
        saved_connection = await db_service.save_connection(connection)

        # Remove sensitive information from response
        response_data = saved_connection.dict()
        response_data.pop('password_key_vault_name', None)

        return jsonify({
            "success": True,
            "data": response_data,
            "message": "Connection created successfully"
        })

    except Exception as e:
        logger.error(f"Failed to create connection: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@connections_bp.route('/test', methods=['POST'])
async def test_connection():
    """Test a database connection"""
    try:
        data = await request.get_json()
        logger.info(f"Received test connection request for server: {data.get('server')} database: {data.get('database')}")

        # Validate incoming data
        if not all(key in data for key in ['connection_type', 'server', 'database', 'username', 'password']):
            logger.warning("Test connection request missing required fields")
            return jsonify({
                "success": False,
                "error": "Missing required connection fields"
            }), 400

        test_request = TestConnectionRequest(**data)

        # Create temporary connection object for testing
        temp_connection = DatabaseConnection(
            id="test",
            name="test",
            connection_type=test_request.connection_type,
            server=test_request.server,
            database=test_request.database,
            username=test_request.username,
            password_key_vault_name="test",  # Not used for testing
            port=test_request.port,
            additional_params=test_request.additional_params
        )

        # Get database service and test connection
        try:
            db_service = get_database_service()
            success, message = await db_service.test_connection(temp_connection, test_request.password)

            return jsonify({
                "success": True,
                "data": {
                    "connection_successful": success,
                    "message": message
                }
            })
        except Exception as service_err:
            logger.error(f"Database service error: {service_err}")
            return jsonify({
                "success": False,
                "error": f"Service error: {str(service_err)}"
            }), 500

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return jsonify({
            "success": False,
            "error": f"Request processing failed: {str(e)}"
        }), 500

@connections_bp.route('/<connection_id>/test', methods=['POST'])
async def test_existing_connection(connection_id: str):
    """Test an existing database connection and update its status"""
    try:
        db_service = get_database_service()

        # Get the existing connection
        connection = await db_service.get_connection_by_id(connection_id)
        if not connection:
            return jsonify({
                "success": False,
                "error": "Connection not found"
            }), 404

        # Get password from Key Vault
        password = await db_service.get_password_from_keyvault(connection.password_key_vault_name)

        # Test the connection
        test_result, test_message = await db_service.test_connection(connection, password)

        # Update connection status based on test result
        connection.status = ConnectionStatus.ACTIVE if test_result else ConnectionStatus.ERROR
        connection.test_connection_result = test_message

        # Save updated connection
        await db_service.save_connection(connection)

        return jsonify({
            "success": True,
            "data": {
                "connection_successful": test_result,
                "status": connection.status.value,
                "message": test_message
            }
        })

    except Exception as e:
        logger.error(f"Failed to test connection {connection_id}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@connections_bp.route('/<connection_id>', methods=['GET'])
async def get_connection(connection_id: str):
    """Get a specific database connection"""
    try:
        db_service = get_database_service()
        connection = await db_service.get_connection_by_id(connection_id)

        if not connection:
            return jsonify({
                "success": False,
                "error": "Connection not found"
            }), 404

        # Remove sensitive information from response
        response_data = connection.dict()
        response_data.pop('password_key_vault_name', None)

        return jsonify({
            "success": True,
            "data": response_data
        })

    except Exception as e:
        logger.error(f"Failed to get connection: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@connections_bp.route('/<connection_id>', methods=['DELETE'])
async def delete_connection(connection_id: str):
    """Delete a database connection"""
    try:
        db_service = get_database_service()
        success = await db_service.delete_connection(connection_id)

        if not success:
            return jsonify({
                "success": False,
                "error": "Connection not found"
            }), 404

        return jsonify({
            "success": True,
            "message": "Connection deleted successfully"
        })

    except Exception as e:
        logger.error(f"Failed to delete connection: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@connections_bp.route('/<connection_id>/tables', methods=['GET'])
async def get_connection_tables(connection_id: str):
    """Get tables for a specific connection"""
    try:
        db_service = get_database_service()
        tables = await db_service.get_tables(connection_id)

        return jsonify({
            "success": True,
            "data": tables
        })

    except Exception as e:
        logger.error(f"Failed to get tables: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@connections_bp.route('/<connection_id>/tables/<table_name>/columns', methods=['GET'])
async def get_table_columns(connection_id: str, table_name: str):
    """Get columns for a specific table"""
    try:
        db_service = get_database_service()
        columns = await db_service.get_table_columns(connection_id, table_name)

        return jsonify({
            "success": True,
            "data": columns
        })

    except Exception as e:
        logger.error(f"Failed to get table columns: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
 