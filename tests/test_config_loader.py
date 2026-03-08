from __future__ import annotations

from pathlib import Path

import pytest

from config import loader


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_config_with_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "app.yaml"
    _write_yaml(
        config_path,
        """
environment: local
logging:
  level: INFO
  log_dir: logs
  log_file: logs/app.log
""",
    )

    monkeypatch.setenv("APP_ENVIRONMENT", "test")
    monkeypatch.setenv("APP_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("APP_LOG_DIR", "var/logs")
    monkeypatch.setenv("APP_LOG_FILE", "var/logs/app.log")

    config = loader.load_config(config_path)

    assert config.environment == "test"
    assert config.logging.level == "DEBUG"
    assert config.logging.log_dir == "var/logs"
    assert config.logging.log_file == "var/logs/app.log"


def test_resolve_config_path_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_CONFIG_PATH", "/tmp/app.yaml")
    assert loader._resolve_config_path(None) == Path("/tmp/app.yaml")


def test_env_overrides_database_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "app.yaml"
    _write_yaml(
        config_path,
        """
environment: local
database:
  url: postgresql+psycopg2://from-file
""",
    )

    monkeypatch.setenv("APP_DB_URL", "postgresql+psycopg2://from-env")

    config = loader.load_config(config_path)

    assert config.database.url == "postgresql+psycopg2://from-env"


def test_env_overrides_tushare_tokens(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "app.yaml"
    _write_yaml(
        config_path,
        """
environment: local
tushare:
  token_private: from-file-private
  token_public: from-file-public
""",
    )

    monkeypatch.setenv("APP_TUSHARE_TOKEN_PRIVATE", "from-env-private")
    monkeypatch.setenv("APP_TUSHARE_TOKEN_PUBLIC", "from-env-public")

    config = loader.load_config(config_path)

    assert config.tushare.token_private == "from-env-private"
    assert config.tushare.token_public == "from-env-public"


def test_read_yaml_missing(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError):
        loader._read_yaml(missing_path)


def test_read_yaml_invalid_top_level(tmp_path: Path) -> None:
    invalid_path = tmp_path / "invalid.yaml"
    _write_yaml(invalid_path, "- item1\n- item2\n")

    with pytest.raises(ValueError, match="mapping"):
        loader._read_yaml(invalid_path)
