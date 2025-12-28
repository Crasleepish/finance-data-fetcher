from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from config.settings import AppConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "app.yaml"


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load AppConfig from YAML file with environment variable overrides."""
    resolved_path = _resolve_config_path(config_path)
    raw_config = _read_yaml(resolved_path)
    merged_config = _apply_env_overrides(raw_config)
    return AppConfig.model_validate(merged_config)


def _resolve_config_path(config_path: Path | None) -> Path:
    """Resolve config path from explicit arg, env var, or default."""
    if config_path is not None:
        return config_path

    env_path = os.environ.get("APP_CONFIG_PATH")
    if env_path:
        return Path(env_path)

    return DEFAULT_CONFIG_PATH


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping file and return a dict."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    if raw is None:
        return {}

    if not isinstance(raw, dict):
        raise ValueError("Config file must contain a mapping at the top level")

    return raw


def _apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    """Merge known environment overrides onto the raw config mapping."""
    merged = dict(config)

    environment = os.environ.get("APP_ENVIRONMENT")
    if environment:
        merged["environment"] = environment

    database_config = dict(merged.get("database", {}))
    db_url = os.environ.get("APP_DB_URL")
    if db_url:
        database_config["url"] = db_url

    if database_config:
        merged["database"] = database_config

    tushare_config = dict(merged.get("tushare", {}))
    tushare_token = os.environ.get("APP_TUSHARE_TOKEN")
    if tushare_token:
        tushare_config["token"] = tushare_token
    tushare_exchange = os.environ.get("APP_TUSHARE_EXCHANGE")
    if tushare_exchange:
        tushare_config["exchange"] = tushare_exchange
    if tushare_config:
        merged["tushare"] = tushare_config

    logging_config = dict(merged.get("logging", {}))
    pipeline_mapping_path = os.environ.get("APP_PIPELINE_MAPPING_PATH")
    if pipeline_mapping_path:
        merged["pipeline_mapping_path"] = pipeline_mapping_path
    log_level = os.environ.get("APP_LOG_LEVEL")
    if log_level:
        logging_config["level"] = log_level

    log_dir = os.environ.get("APP_LOG_DIR")
    if log_dir:
        logging_config["log_dir"] = log_dir

    log_file = os.environ.get("APP_LOG_FILE")
    if log_file:
        logging_config["log_file"] = log_file

    if logging_config:
        merged["logging"] = logging_config

    return merged
