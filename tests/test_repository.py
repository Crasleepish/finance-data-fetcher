from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.engine import Engine

from infra.db.engine import transaction
from infra.db.repository import Repository
from infra.db.tables import test_messages


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
