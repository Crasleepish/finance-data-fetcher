from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

import pytest
from sqlalchemy import Column, MetaData, String, Table, insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from infra.db.repository import Repository
from infra.db.tables import stock_code_change


class FakeDialect:
    name = "postgresql"

    class _Preparer:
        def quote(self, identifier: str) -> str:
            return f'"{identifier}"'

    identifier_preparer = _Preparer()


class FakeResult:
    rowcount = 1


class FakeConnection:
    def __init__(self) -> None:
        self.dialect = FakeDialect()
        self.executed: list[Any] = []
        self.fail_on_insert = False

    def execute(self, stmt: Any) -> FakeResult:
        self.executed.append(stmt)
        text = str(stmt)
        if self.fail_on_insert and text.startswith("INSERT"):
            raise RuntimeError("insert failed")
        return FakeResult()


class FakeEngine:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def connect(self) -> FakeConnection:
        return self.connection


@pytest.fixture
def repo_fixture(monkeypatch: pytest.MonkeyPatch) -> tuple[Repository, FakeConnection]:
    connection = FakeConnection()
    engine = FakeEngine(connection)
    table = Table("stock_code_change", MetaData(), Column("trade_code", String))
    repo = Repository(engine=engine, table=table, serialize_replacement=True)  # type: ignore[arg-type]

    @contextmanager
    def fake_transaction(_engine: Any) -> Iterator[FakeConnection]:
        connection.executed = []
        yield connection

    monkeypatch.setattr("infra.db.repository.transaction", fake_transaction)
    return repo, connection


def test_replacement_lock_acquired_before_delete(
    repo_fixture: tuple[Repository, FakeConnection],
) -> None:
    repo, connection = repo_fixture

    repo.replace_all([{"trade_code": "600000.SH"}])

    statements = [str(stmt) for stmt in connection.executed]
    lock_index = next(i for i, stmt in enumerate(statements) if "LOCK TABLE" in stmt)
    delete_index = next(i for i, stmt in enumerate(statements) if stmt.startswith("DELETE"))
    insert_index = next(i for i, stmt in enumerate(statements) if stmt.startswith("INSERT"))
    assert lock_index < delete_index < insert_index
    assert '"stock_code_change"' in statements[lock_index]
    assert "SHARE ROW EXCLUSIVE" in statements[lock_index]


def test_fake_insert_failure_error_propagates(
    repo_fixture: tuple[Repository, FakeConnection],
) -> None:
    # Fake transport only demonstrates error propagation, not real rollback.
    repo, connection = repo_fixture
    connection.fail_on_insert = True

    with pytest.raises(RuntimeError, match="insert failed"):
        repo.replace_all([{"trade_code": "600000.SH"}])


def test_serialize_flag_false_skips_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection()
    engine = FakeEngine(connection)
    table = Table("stock_code_change", MetaData(), Column("trade_code", String))
    repo = Repository(engine=engine, table=table)  # type: ignore[arg-type]

    @contextmanager
    def fake_transaction(_engine: Any) -> Iterator[FakeConnection]:
        connection.executed = []
        yield connection

    monkeypatch.setattr("infra.db.repository.transaction", fake_transaction)

    repo.replace_all([{"trade_code": "600000.SH"}])

    assert all("LOCK TABLE" not in str(stmt) for stmt in connection.executed)


# ---------------------------------------------------------------------------
# Real disposable PostgreSQL integration (testcontainers; auto-skips w/o Docker).
# ---------------------------------------------------------------------------


def _seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "trade_code": "001914.SZ",
            "code": "000043.SZ",
            "name": "招商积余",
            "start_date": date(1991, 1, 1),
            "end_date": date(2015, 12, 31),
        },
        {
            "trade_code": "001914.SZ",
            "code": "001914.SZ",
            "name": "招商积余",
            "start_date": date(2016, 1, 1),
            "end_date": None,
        },
    ]


def _read_rows(engine: Engine) -> list[dict[str, Any]]:
    stmt = select(stock_code_change).order_by(
        stock_code_change.c.trade_code,
        stock_code_change.c.code,
        stock_code_change.c.start_date,
    )
    with engine.begin() as connection:
        return [dict(row) for row in connection.execute(stmt).mappings()]


def test_replace_all_rolls_back_after_delete_on_insert_failure(
    postgres_engine: Engine,
) -> None:
    original = _seed_rows()
    with postgres_engine.begin() as connection:
        connection.execute(insert(stock_code_change), original)

    repo = Repository(engine=postgres_engine, table=stock_code_change, serialize_replacement=True)
    # Duplicate primary key (same trade_code/code/start_date) -> DELETE succeeds,
    # INSERT raises inside the same transaction.
    invalid = [
        {
            "trade_code": "600000.SH",
            "code": "600000.SH",
            "name": "浦发银行",
            "start_date": date(2000, 1, 1),
            "end_date": None,
        },
        {
            "trade_code": "600000.SH",
            "code": "600000.SH",
            "name": "浦发银行",
            "start_date": date(2000, 1, 1),
            "end_date": None,
        },
    ]

    with pytest.raises(DBAPIError):
        repo.replace_all(invalid)

    assert _read_rows(postgres_engine) == original

    # Connection/engine still usable for a subsequent successful replacement.
    replacement = [
        {
            "trade_code": "600000.SH",
            "code": "600000.SH",
            "name": "浦发银行",
            "start_date": date(2000, 1, 1),
            "end_date": None,
        }
    ]
    affected = repo.replace_all(replacement)
    assert affected == 1
    assert _read_rows(postgres_engine) == replacement
