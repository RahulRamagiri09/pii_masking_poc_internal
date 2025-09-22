from quart import Quart, request, jsonify, send_from_directory
from quart_cors import cors
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.cosmos import CosmosClient
import os
import logging
from dotenv import load_dotenv
from logging.config import dictConfig

# Add this configuration before initializing the app
dictConfig({
    'version': 1,
    'loggers': {
        'quart.app': {
            'level': 'ERROR',
        },
        'quart.serving': {
            'level': 'ERROR',
        },
    }
})

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)


class AppConfig:
   """Application configuration using Azure services"""

   def __init__(self):
        # Log authentication method being used
        if os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_TENANT_ID") and os.getenv("AZURE_CLIENT_SECRET"):
            logger.info("Using service principal authentication")
        elif os.getenv("MSI_ENDPOINT"):
            logger.info("Using managed identity authentication")
        else:
            logger.info("Attempting to use Azure CLI or other authentication methods")

        try:
            # Attempt to authenticate with DefaultAzureCredential
            self.credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        except Exception as e:
            logger.warning(f"Authentication with DefaultAzureCredential failed: {e}")
            # For development purposes only - mock credential
            self.credential = None
            logger.warning("Using mock credential for development - LIMITED FUNCTIONALITY")

        # Load endpoints and configs
        self.cosmos_endpoint = os.getenv("AZURE_COSMOS_ENDPOINT")
        self.cosmos_database = os.getenv("AZURE_COSMOS_DATABASE", "pii-masking-db")
        self.key_vault_url = os.getenv("AZURE_KEY_VAULT_URL")

        # Initialize Azure clients
        self.init_cosmos_client()



   def _init_cosmos_client(self):

      """Initialize Cosmos DB client with managed identity or development mode."""

      try:

         cosmos_key = os.getenv("AZURE_COSMOS_KEY")

         if self._credential is None and cosmos_key:

               # Development mode with key authentication

               logger.info("DEVELOPMENT MODE: Using direct key for Cosmos DB")

               self.cosmos_client = CosmosClient(

                  url=self._cosmos_endpoint,

                  credential=cosmos_key

               )

               self.cosmos_db = self.cosmos_client.get_database_client(self._cosmos_database)

               logger.info("Cosmos DB client initialized successfully with key authentication")

         elif self._credential is None and not cosmos_key:

               # Development mode without key

               logger.warning("DEVELOPMENT MODE: No Cosmos DB key provided")

               logger.warning("DEVELOPMENT MODE: No Cosmos DB connection, database operations will fail")

         else:

               # Production mode with credential

               self.cosmos_client = CosmosClient(

                  url=self._cosmos_endpoint,

                  credential=self._credential

               )

               self.cosmos_db = self.cosmos_client.get_database_client(self._cosmos_database)

               logger.info("Cosmos DB client initialized successfully with credential")

      except Exception as e:

         logger.error(f"Failed to initialize Cosmos DB client: {e}")

         self.cosmos_client = None

         self.cosmos_db = None


   def _init_keyvault_client(self):

      """Initialize Key Vault client with managed identity."""

      try:

         if self._credential is None:

               logger.warning("DEVELOPMENT MODE: Not connecting to Key Vault")

               logger.warning("DEVELOPMENT MODE: No Key Vault connection, secret operations will fail")

         else:

               self.keyvault_client = SecretClient(

                  vault_url=self._key_vault_url,

                  credential=self._credential

               )

               logger.info("Key Vault client initialized successfully")

      except Exception as e:

         logger.error(f"Failed to initialize Key Vault client: {e}")

         self.keyvault_client = None


   def create_app():

      """Application factory."""

      app = Quart(__name__, static_folder="../frontend/build", static_url_path="")

      # Apply CORS

      app = cors(app, allow_origins=os.getenv("CORS_ORIGINS", "").split(","))

      # Initial app configuration

      app.config["APP_CONFIG"] = AppConfig()

      # Register blueprints

      from routes.connections import connections_bp

      from routes.workflows import workflows_bp

      from routes.masking import masking_bp

      app.register_blueprint(connections_bp, url_prefix="/api/connections")

      app.register_blueprint(workflows_bp, url_prefix="/api/workflows")

      app.register_blueprint(masking_bp, url_prefix="/api/masking")

      @app.route("/health")

      async def health_check():

         """Health check endpoint"""

         return jsonify({

               "status": "healthy",

               "cosmos_db": app.config["APP_CONFIG"].cosmos_client is not None,

               "key_vault": app.config["APP_CONFIG"].keyvault_client is not None

         })

      @app.errorhandler(Exception)

      async def handle_exception(e):

         """Global exception handler"""

         logger.error(f"Unhandled exception: {e}")

         return jsonify({"error": "Internal server error"}), 500

      @app.route("/<path:path>")

      async def serve_frontend(path):

         """Serve React build files"""

         try:

               # Check if the path exists as a static file

               static_file_path = os.path.join(app.static_folder, path)

               if os.path.exists(static_file_path) and os.path.isfile(static_file_path):

                  return await app.send_static_file(path)

               else:

                  # Return index.html for all other routes (SPA routing)

                  return await app.send_static_file("index.html")

         except Exception as e:

               logger.error(f"Error serving static file: {e}")

               return jsonify({"error": "File not found"}), 404

      return app


   if __name__ == "__main__":

      app = create_app()

      # Import and run debug checks if in debug mode

      if os.getenv("DEBUG", "False").lower() == "true":

         try:

               from debug_routes import print_debug_info

               print_debug_info(app)

         except Exception as e:

               logger.error(f"Debug info error: {e}")

      app.run(debug=os.getenv("DEBUG", "False").lower() == "true", port=5000)
   
