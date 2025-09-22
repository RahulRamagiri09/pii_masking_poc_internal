#!/usr/bin/env python3
"""
Configuration validation script to check if environment variables are loaded correctly.
"""
import os
from pii_masking.core.config import settings

def check_config():
    print("FastAPI Dynamic Configuration Check")
    print("=" * 50)

    # Database settings
    print("\nDatabase Configuration:")
    print(f"  DATABASE_URL: {settings.DATABASE_URL[:30]}...")
    print(f"  DATABASE_ECHO: {settings.DATABASE_ECHO}")
    print(f"  DATABASE_POOL_SIZE: {settings.DATABASE_POOL_SIZE}")
    print(f"  DATABASE_MAX_OVERFLOW: {settings.DATABASE_MAX_OVERFLOW}")

    # Security settings
    print("\nSecurity Configuration:")
    print(f"  SECRET_KEY: {'***' if settings.SECRET_KEY else 'NOT SET'}")
    print(f"  ALGORITHM: {settings.ALGORITHM}")
    print(f"  ACCESS_TOKEN_EXPIRE_MINUTES: {settings.ACCESS_TOKEN_EXPIRE_MINUTES}")
    print(f"  BCRYPT_ROUNDS: {settings.BCRYPT_ROUNDS}")

    # Server settings
    print("\nServer Configuration:")
    print(f"  HOST: {settings.HOST}")
    print(f"  PORT: {settings.PORT}")
    print(f"  RELOAD: {settings.RELOAD}")
    print(f"  DEBUG: {settings.DEBUG}")

    # API settings
    print("\nAPI Configuration:")
    print(f"  API_PREFIX: {settings.API_PREFIX}")
    print(f"  DEFAULT_PAGE_SIZE: {settings.DEFAULT_PAGE_SIZE}")
    print(f"  MAX_PAGE_SIZE: {settings.MAX_PAGE_SIZE}")

    # CORS settings
    print("\nCORS Configuration:")
    print(f"  BACKEND_CORS_ORIGINS: {settings.BACKEND_CORS_ORIGINS}")
    print(f"  ALLOW_CREDENTIALS: {settings.ALLOW_CREDENTIALS}")

    # App settings
    print("\nApp Configuration:")
    print(f"  PROJECT_NAME: {settings.PROJECT_NAME}")
    print(f"  VERSION: {settings.VERSION}")

    # Environment file check
    env_file_exists = os.path.exists('.env')
    print(f"\nEnvironment File:")
    print(f"  .env exists: {env_file_exists}")

    if not env_file_exists:
        print("  WARNING: Consider creating .env file from .env.example")

    print("\nConfiguration check completed!")

if __name__ == "__main__":
    check_config()