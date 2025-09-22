from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Database Configuration
    DATABASE_URL: str
    DATABASE_ECHO: bool
    DATABASE_POOL_SIZE: int
    DATABASE_MAX_OVERFLOW: int

    # JWT Authentication
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    # App Configuration
    PROJECT_NAME: str
    VERSION: str
    DEBUG: bool
    API_PREFIX: str

    # Server Configuration
    HOST: str
    PORT: int
    RELOAD: bool

    # CORS Configuration
    BACKEND_CORS_ORIGINS: List[str]
    ALLOW_CREDENTIALS: bool
    ALLOW_METHODS: List[str]
    ALLOW_HEADERS: List[str]

    # Security
    BCRYPT_ROUNDS: int
    ALLOW_PUBLIC_ROLE_CREATION: bool

    # Pagination
    DEFAULT_PAGE_SIZE: int
    MAX_PAGE_SIZE: int

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()