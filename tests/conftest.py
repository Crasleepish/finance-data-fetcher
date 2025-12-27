from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from docker import DockerClient
from docker.errors import DockerException
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
    if not _docker_available():
        pytest.skip("Docker unavailable; skipping integration database tests.")
    with PostgresContainer("postgres:16") as postgres:
        engine = create_engine(postgres.get_connection_url())
        metadata.create_all(engine)
        yield engine
        metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(request: pytest.FixtureRequest) -> Iterator[None]:
    if "postgres_engine" not in request.fixturenames:
        yield
        return

    engine = request.getfixturevalue("postgres_engine")
    with engine.begin() as connection:
        for table in reversed(metadata.sorted_tables):
            connection.execute(table.delete())
    yield


def _docker_available() -> bool:
    """Return True if Docker daemon is reachable."""
    try:
        client = DockerClient.from_env()
        client.ping()
        return True
    except DockerException:
        return False
