from __future__ import annotations

from pydantic import BaseModel, Field


class LoggingConfig(BaseModel):
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
    url: str = Field(default="postgresql+psycopg2://myuser:xjqxz214@192.168.56.101:5432/mydb")


class AppConfig(BaseModel):
    environment: str = Field(default="local")
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
