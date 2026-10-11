from __future__ import annotations

from pathlib import Path

import pytest

from config import loader


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _clear_ftshare_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "FTSHARE_API_KEY",
        "APP_FTSHARE_API_KEY",
        "FTSHARE_BASE_URL",
        "APP_FTSHARE_BASE_URL",
        "APP_FTSHARE_BATCH_SIZE",
        "APP_FTSHARE_MAX_URL_LENGTH",
        "APP_FTSHARE_MAX_REQUESTS",
        "APP_FTSHARE_TIMEOUT",
    ):
        monkeypatch.delenv(name, raising=False)


def test_ftshare_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ftshare_env(monkeypatch)
    config_path = tmp_path / "app.yaml"
    _write_yaml(config_path, "environment: local\n")

    config = loader.load_config(config_path)

    assert config.ftshare.api_key == ""
    assert config.ftshare.base_url == "https://market.ft.tech/gateway"
    assert config.ftshare.batch_size == 400
    assert config.ftshare.max_url_length == 5000
    assert config.ftshare.max_requests == 50
    assert config.ftshare.timeout == 30.0


def test_ftshare_standard_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ftshare_env(monkeypatch)
    monkeypatch.setenv("FTSHARE_API_KEY", "standard-key")
    monkeypatch.setenv("FTSHARE_BASE_URL", "https://std.example/gateway")
    config_path = tmp_path / "app.yaml"
    _write_yaml(config_path, "environment: local\n")

    config = loader.load_config(config_path)

    assert config.ftshare.api_key == "standard-key"
    assert config.ftshare.base_url == "https://std.example/gateway"


def test_ftshare_app_env_takes_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ftshare_env(monkeypatch)
    monkeypatch.setenv("FTSHARE_API_KEY", "standard-key")
    monkeypatch.setenv("APP_FTSHARE_API_KEY", "app-key")
    monkeypatch.setenv("FTSHARE_BASE_URL", "https://std.example/gateway")
    monkeypatch.setenv("APP_FTSHARE_BASE_URL", "https://app.example/gateway")
    config_path = tmp_path / "app.yaml"
    _write_yaml(config_path, "environment: local\n")

    config = loader.load_config(config_path)

    assert config.ftshare.api_key == "app-key"
    assert config.ftshare.base_url == "https://app.example/gateway"


def test_ftshare_yaml_values_preserved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ftshare_env(monkeypatch)
    config_path = tmp_path / "app.yaml"
    _write_yaml(
        config_path,
        "environment: local\nftshare:\n  api_key: file-key\n  batch_size: 10\n",
    )

    config = loader.load_config(config_path)

    assert config.ftshare.api_key == "file-key"
    assert config.ftshare.batch_size == 10


def test_ftshare_numeric_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ftshare_env(monkeypatch)
    monkeypatch.setenv("APP_FTSHARE_BATCH_SIZE", "50")
    monkeypatch.setenv("APP_FTSHARE_MAX_URL_LENGTH", "1234")
    monkeypatch.setenv("APP_FTSHARE_MAX_REQUESTS", "7")
    monkeypatch.setenv("APP_FTSHARE_TIMEOUT", "5.5")
    config_path = tmp_path / "app.yaml"
    _write_yaml(config_path, "environment: local\n")

    config = loader.load_config(config_path)

    assert config.ftshare.batch_size == 50
    assert config.ftshare.max_url_length == 1234
    assert config.ftshare.max_requests == 7
    assert config.ftshare.timeout == 5.5


def test_ftshare_key_not_in_repr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ftshare_env(monkeypatch)
    monkeypatch.setenv("FTSHARE_API_KEY", "super-secret-token")
    config_path = tmp_path / "app.yaml"
    _write_yaml(config_path, "environment: local\n")

    config = loader.load_config(config_path)

    assert "super-secret-token" not in repr(config.ftshare)
    assert "super-secret-token" not in repr(config)


@pytest.mark.parametrize(
    "content",
    [
        "ftshare:\n  batch_size: 0\n",
        "ftshare:\n  batch_size: 401\n",
        "ftshare:\n  max_url_length: 0\n",
        "ftshare:\n  max_url_length: 5001\n",
        "ftshare:\n  max_requests: 0\n",
        "ftshare:\n  max_requests: 51\n",
        "ftshare:\n  timeout: 0\n",
        "ftshare:\n  timeout: -5\n",
    ],
)
def test_ftshare_config_cap_violations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str
) -> None:
    _clear_ftshare_env(monkeypatch)
    config_path = tmp_path / "app.yaml"
    _write_yaml(config_path, "environment: local\n" + content)

    with pytest.raises(ValueError):
        loader.load_config(config_path)
