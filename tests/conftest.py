from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from testcontainers.postgres import PostgresContainer

from infra.db.tables import metadata


@pytest.fixture(scope="session", autouse=True)
def docker_env(tmp_path_factory: pytest.TempPathFactory) -> Path:
    config_dir = tmp_path_factory.mktemp("docker-config")
    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps({"auths": {}}), encoding="utf-8")
    os.environ["DOCKER_CONFIG"] = str(config_dir)
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    return config_dir


@pytest.fixture(scope="session")
def postgres_engine(docker_env: Path) -> Iterator[Engine]:
    with PostgresContainer("postgres:16") as postgres:
        engine = create_engine(postgres.get_connection_url())
        metadata.create_all(engine)
        yield engine
        metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(postgres_engine: Engine) -> Iterator[None]:
    with postgres_engine.begin() as connection:
        for table in reversed(metadata.sorted_tables):
            connection.execute(table.delete())
    yield
