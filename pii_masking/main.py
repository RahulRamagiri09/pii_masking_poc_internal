from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

from .auth.routes import auth, role, user
from .masking.routes import connection, workflow, masking
from .core.config import settings

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    debug=settings.DEBUG,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=settings.ALLOW_CREDENTIALS,
    allow_methods=settings.ALLOW_METHODS,
    allow_headers=settings.ALLOW_HEADERS,
)

app.include_router(auth.router, prefix=f"{settings.API_PREFIX}/auth", tags=["authentication"])
app.include_router(role.router, prefix=f"{settings.API_PREFIX}/roles", tags=["roles"])
app.include_router(user.router, prefix=f"{settings.API_PREFIX}/users", tags=["users"])

# Masking routes
app.include_router(connection.router, prefix=f"{settings.API_PREFIX}/connections", tags=["connections"])
app.include_router(workflow.router, prefix=f"{settings.API_PREFIX}/workflows", tags=["workflows"])
app.include_router(masking.router, prefix=f"{settings.API_PREFIX}/masking", tags=["masking"])


@app.get("/")
async def root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}"}


@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "message": "PII Masking API is running"
    }