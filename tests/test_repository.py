from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from testcontainers.postgres import PostgresContainer

from infra.db.engine import transaction
from infra.db.repository import Repository
from infra.db.tables import metadata, test_messages


@pytest.fixture(scope="session")
def docker_config_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    config_dir = tmp_path_factory.mktemp("docker-config")
    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps({"auths": {}}), encoding="utf-8")
    os.environ["DOCKER_CONFIG"] = str(config_dir)
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    return config_dir


@pytest.fixture(scope="session")
def postgres_engine(docker_config_dir: Path) -> Iterator[Engine]:
    with PostgresContainer("postgres:16") as postgres:
        engine = create_engine(postgres.get_connection_url())
        metadata.create_all(engine)
        yield engine
        metadata.drop_all(engine)
        engine.dispose()


def test_repository_insert(postgres_engine: Engine) -> None:
    repo = Repository(engine=postgres_engine, table=test_messages)

    inserted = repo.insert_batch(
        [
            {"id": 1, "message": "hello"},
            {"id": 2, "message": "world"},
        ]
    )

    assert inserted == 2

    with transaction(postgres_engine) as connection:
        rows = connection.execute(select(test_messages.c.message).order_by(test_messages.c.id))
        messages = [row.message for row in rows]

    assert messages == ["hello", "world"]


def test_repository_upsert(postgres_engine: Engine) -> None:
    repo = Repository(engine=postgres_engine, table=test_messages)

    repo.insert_batch([{"id": 3, "message": "before"}])

    updated = repo.upsert_batch(
        [{"id": 3, "message": "after"}],
        unique_keys=["id"],
    )

    assert updated == 1

    with transaction(postgres_engine) as connection:
        row = connection.execute(
            select(test_messages.c.message).where(test_messages.c.id == 3)
        ).one()

    assert row.message == "after"
