import os
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "datagen-api"
    app_version: str = "1.0.0"
    debug: bool = True
    host: str = "0.0.0.0"
    port: int = 8000

    # PostgreSQL configuration settings
    pg_host: str = "localhost"
    pg_port: int = 5434
    pg_db: str = "thelook_db"
    pg_user: str = "db_user"
    pg_password: str = "db_password"
    pg_schema: str = "demo"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# Dynamic fallback to standard Railway PG environment variables
if os.getenv("PGHOST"):
    settings.pg_host = os.getenv("PGHOST")
if os.getenv("PGPORT"):
    settings.pg_port = int(os.getenv("PGPORT"))
if os.getenv("PGDATABASE"):
    settings.pg_db = os.getenv("PGDATABASE")
if os.getenv("PGUSER"):
    settings.pg_user = os.getenv("PGUSER")
if os.getenv("PGPASSWORD"):
    settings.pg_password = os.getenv("PGPASSWORD")
