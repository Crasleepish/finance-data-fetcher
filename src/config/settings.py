from __future__ import annotations

from pydantic import BaseModel, Field


class LoggingConfig(BaseModel):
    """Logging configuration values for the service runtime."""

    level: str = Field(default="INFO")
    log_dir: str = Field(default="logs")
    log_file: str = Field(default="logs/app.log")
    rotation_when: str = Field(default="midnight")
    rotation_interval: int = Field(default=1)
    backup_count: int = Field(default=30)
    encoding: str = Field(default="utf-8")
    console: bool = Field(default=True)
    log_format: str = Field(default="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    date_format: str = Field(default="%Y-%m-%d %H:%M:%S")


class DatabaseConfig(BaseModel):
    """Database connectivity configuration."""

    url: str = Field(default="postgresql+psycopg2://myuser:xjqxz214@192.168.56.101:5432/mydb")


class TushareConfig(BaseModel):
    """Tushare integration configuration (token + exchange defaults)."""

    token: str = Field(default="")
    exchange: str = Field(default="SSE")


class AppConfig(BaseModel):
    """Root application configuration assembled from file + environment."""

    environment: str = Field(default="local")
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    tushare: TushareConfig = Field(default_factory=TushareConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    pipeline_mapping_path: str = Field(default="config/task_pipeline_mapping.py")
