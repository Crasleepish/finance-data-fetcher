from __future__ import annotations

import pytest
from sqlalchemy.engine import Engine

from core.clean.policy import ErrorMode
from core.clean.typed_cleaner import TypedCleaner
from infra.db.repository import Repository
from infra.db.tables import test_messages


def test_cleaner_fail_chunk() -> None:
    cleaner = TypedCleaner(
        field_map={"id": "id", "message": "message"},
        type_map={"id": int, "message": str},
        required_fields={"id", "message"},
        casts={"id": int},
        error_mode=ErrorMode.FAIL_CHUNK,
    )

    raw_batch = [{"id": "not-an-int", "message": "ok"}]

    with pytest.raises(ValueError):
        cleaner.clean(raw_batch)


def test_cleaner_output_to_repo(postgres_engine: Engine) -> None:
    cleaner = TypedCleaner(
        field_map={"id": "id", "message": "message"},
        type_map={"id": int, "message": str},
        required_fields={"id", "message"},
        casts={"id": int},
        error_mode=ErrorMode.FAIL_CHUNK,
    )
    repo = Repository(engine=postgres_engine, table=test_messages)

    raw_batch = [{"id": "1", "message": "hello"}]
    normalized = cleaner.clean(raw_batch)

    inserted = repo.insert_batch(normalized)

    assert inserted == 1
