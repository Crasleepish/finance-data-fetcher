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

    token_private: str = Field(default="")
    token_public: str = Field(default="")
    exchange: str = Field(default="SSE")


class IndexDataConfig(BaseModel):
    """Index universe configuration by data source."""

    stock: str = Field(default="")
    bond: str = Field(default="")
    gold: str = Field(default="")


class FundDataConfig(BaseModel):
    """Fund configuration for pipeline inputs."""

    money: str = Field(default="")


class GoldDataConfig(BaseModel):
    """Gold derivatives fetcher configuration."""

    cftc_history_url_template: str = Field(
        default="https://www.cftc.gov/files/dea/history/com_disagg_txt_{year}.zip"
    )
    barchart_quotes_url: str = Field(
        default="https://www.barchart.com/proxies/core-api/v1/quotes/get"
    )
    tmp_dir: str = Field(default="/tmp")


class DataConfig(BaseModel):
    """Data configuration for pipeline inputs."""

    index: IndexDataConfig = Field(default_factory=IndexDataConfig)
    fund: FundDataConfig = Field(default_factory=FundDataConfig)


class AppConfig(BaseModel):
    """Root application configuration assembled from file + environment."""

    environment: str = Field(default="local")
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    tushare: TushareConfig = Field(default_factory=TushareConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    gold: GoldDataConfig = Field(default_factory=GoldDataConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    pipeline_mapping_path: str = Field(default="config/task_pipeline_mapping.py")
