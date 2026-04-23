from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.engine import Engine

from infra.db.engine import transaction
from infra.db.repository import Repository
from infra.db.tables import stock_hist_unadj, test_messages


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


def test_stock_hist_update_only_changes_is_st(postgres_engine: Engine) -> None:
    repo = Repository(engine=postgres_engine, table=stock_hist_unadj)
    with transaction(postgres_engine) as connection:
        connection.execute(
            stock_hist_unadj.insert().values(
                stock_code="000001.SZ",
                date=date(2024, 1, 2),
                close=10.5,
                is_st=None,
                is_suspend="N",
            )
        )

    updated = repo.update_batch(
        [
            {
                "stock_code": "000001.SZ",
                "date": date(2024, 1, 2),
                "is_st": 1,
            }
        ],
        key_columns=["stock_code", "date"],
        update_columns=["is_st"],
    )

    assert updated == 1

    with transaction(postgres_engine) as connection:
        row = connection.execute(
            select(stock_hist_unadj.c.close, stock_hist_unadj.c.is_st).where(
                stock_hist_unadj.c.stock_code == "000001.SZ",
                stock_hist_unadj.c.date == date(2024, 1, 2),
            )
        ).one()

    assert row.close == 10.5
    assert row.is_st == 1


def test_stock_hist_update_only_skips_missing_rows(postgres_engine: Engine) -> None:
    repo = Repository(engine=postgres_engine, table=stock_hist_unadj)
    with transaction(postgres_engine) as connection:
        connection.execute(
            stock_hist_unadj.insert().values(
                stock_code="000001.SZ",
                date=date(2024, 1, 2),
                close=10.5,
                is_st=None,
                is_suspend="N",
            )
        )

    updated = repo.update_batch(
        [
            {
                "stock_code": "000001.SZ",
                "date": date(2024, 1, 3),
                "is_st": 0,
            }
        ],
        key_columns=["stock_code", "date"],
        update_columns=["is_st"],
    )

    assert updated == 0

    with transaction(postgres_engine) as connection:
        rows = connection.execute(
            select(stock_hist_unadj.c.stock_code, stock_hist_unadj.c.date)
        ).all()

    assert rows == [("000001.SZ", date(2024, 1, 2))]


def test_stock_hist_upsert_preserves_existing_is_st(postgres_engine: Engine) -> None:
    repo = Repository(engine=postgres_engine, table=stock_hist_unadj)
    with transaction(postgres_engine) as connection:
        connection.execute(
            stock_hist_unadj.insert().values(
                stock_code="000001.SZ",
                date=date(2024, 1, 2),
                close=10.0,
                is_st=1,
                is_suspend="N",
            )
        )

    updated = repo.upsert_batch(
        [
            {
                "stock_code": "000001.SZ",
                "date": date(2024, 1, 2),
                "close": 11.0,
                "is_suspend": "N",
            }
        ],
        unique_keys=["stock_code", "date"],
    )

    assert updated == 1

    with transaction(postgres_engine) as connection:
        row = connection.execute(
            select(stock_hist_unadj.c.close, stock_hist_unadj.c.is_st).where(
                stock_hist_unadj.c.stock_code == "000001.SZ",
                stock_hist_unadj.c.date == date(2024, 1, 2),
            )
        ).one()

    assert row.close == 11.0
    assert row.is_st == 1
