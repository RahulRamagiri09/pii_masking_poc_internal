#!/usr/bin/env python3
"""
Application runner that uses dynamic configuration from environment variables.
"""
import uvicorn
from pii_masking.core.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "pii_masking.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level="debug" if settings.DEBUG else "info",
    )