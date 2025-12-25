from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from config.settings import AppConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "app.yaml"


def load_config(config_path: Path | None = None) -> AppConfig:
    resolved_path = _resolve_config_path(config_path)
    raw_config = _read_yaml(resolved_path)
    merged_config = _apply_env_overrides(raw_config)
    return AppConfig.model_validate(merged_config)


def _resolve_config_path(config_path: Path | None) -> Path:
    if config_path is not None:
        return config_path

    env_path = os.environ.get("APP_CONFIG_PATH")
    if env_path:
        return Path(env_path)

    return DEFAULT_CONFIG_PATH


def _read_yaml(path: Path) -> dict[str, Any]:
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
    merged = dict(config)

    environment = os.environ.get("APP_ENVIRONMENT")
    if environment:
        merged["environment"] = environment

    logging_config = dict(merged.get("logging", {}))
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
