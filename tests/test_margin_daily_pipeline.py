from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from core.calendar.service import TradingCalendarService
from core.clean.margin_daily_cleaner import MarginDailyCleaner
from core.fetch.retry import RetryPolicy
from infra.db.repository import PostgresUpsertStrategy
from infra.db.tables import margin_daily
from infra.fetcher.tushare_margin_daily_fetcher import TushareMarginDailyFetcher
from services.pipelines.margin_daily_pipeline import MarginDailyPipeline

_DEFAULT_FIELDS = "trade_date,exchange_id,rzye,rzmre,rzche,rqye,rqmcl,rzrqye,rqyl"


@dataclass
class FakeCalendarStore:
    dates: set[date]

    def get_bounds(self) -> tuple[date, date] | None:
        return (min(self.dates), max(self.dates)) if self.dates else None

    def get_trade_days(self, start: date, end: date) -> list[date]:
        return sorted(day for day in self.dates if start <= day <= end)

    def is_trade_day(self, day: date) -> bool:
        return day in self.dates

    def prev_trade_day(self, day: date) -> date | None:
        return None

    def next_trade_day(self, day: date) -> date | None:
        return None

    def insert_trade_days(self, days: list[date]) -> int:
        before = len(self.dates)
        self.dates.update(days)
        return len(self.dates) - before


@dataclass(frozen=True)
class FakeSyncer:
    def fetch_trade_days(self, start: date, end: date, exchange: str) -> list[date]:
        return []


@dataclass
class FakeTushareClient:
    calls: list[tuple[str, str, str]] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)

    def margin(
        self,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, object]]:
        self.calls.append((start_date, end_date, fields))
        return list(self.rows)


def _margin_row(
    trade_date: str = "20240102",
    exchange_id: str = "SSE",
    rzye: object = 10000.0,
    rzmre: object = 2000.0,
    rzche: object = 1500.0,
    rqye: object = 500.0,
    rqmcl: object = 100.0,
    rzrqye: object = 10500.0,
    rqyl: object = 50.0,
) -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "exchange_id": exchange_id,
        "rzye": rzye,
        "rzmre": rzmre,
        "rzche": rzche,
        "rqye": rqye,
        "rqmcl": rqmcl,
        "rzrqye": rzrqye,
        "rqyl": rqyl,
    }


def test_margin_fetcher_uses_date_range_and_default_fields() -> None:
    client = FakeTushareClient(rows=[_margin_row()])
    fetcher = TushareMarginDailyFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == [_margin_row()]
    assert client.calls == [("20240102", "20240105", _DEFAULT_FIELDS)]


def test_margin_fetcher_accepts_compact_dates_and_custom_fields() -> None:
    client = FakeTushareClient(rows=[_margin_row()])
    fetcher = TushareMarginDailyFetcher(client=client, retry_policy=RetryPolicy())

    fetcher.fetch(
        {
            "params": {
                "start_date": "20240102",
                "end_date": "20240105",
                "fields": "trade_date,exchange_id,rzrqye",
            }
        }
    )

    assert client.calls == [("20240102", "20240105", "trade_date,exchange_id,rzrqye")]


def test_margin_fetcher_returns_empty_for_empty_response() -> None:
    client = FakeTushareClient(rows=[])
    fetcher = TushareMarginDailyFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == []
    assert client.calls == [("20240102", "20240105", _DEFAULT_FIELDS)]


def test_margin_fetcher_rejects_inverted_date_range() -> None:
    client = FakeTushareClient(rows=[_margin_row()])
    fetcher = TushareMarginDailyFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="start_date must be on or before end_date"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-05",
                    "end_date": "2024-01-02",
                }
            }
        )
    assert client.calls == []


def test_margin_fetcher_rejects_response_beyond_api_row_limit() -> None:
    rows = [_margin_row() for _ in range(4001)]
    client = FakeTushareClient(rows=rows)
    fetcher = TushareMarginDailyFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="exceeded per-request row limit"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-05",
                }
            }
        )


def test_margin_cleaner_normalizes_fields_and_missing_values() -> None:
    cleaner = MarginDailyCleaner()

    cleaned = cleaner.clean(
        [
            _margin_row(rqye=None, rqmcl=None),
            {
                "trade_date": "20240103",
                "exchange_id": "SZSE",
                "rzye": 9999.5,
                "rzmre": None,
                "rzche": None,
                "rzrqye": 9999.5,
            },
        ]
    )

    assert cleaned == [
        {
            "trade_date": date(2024, 1, 2),
            "exchange_id": "SSE",
            "rzye": Decimal("10000.0"),
            "rzmre": Decimal("2000.0"),
            "rzche": Decimal("1500.0"),
            "rqye": None,
            "rqmcl": None,
            "rzrqye": Decimal("10500.0"),
            "rqyl": Decimal("50.0"),
        },
        {
            "trade_date": date(2024, 1, 3),
            "exchange_id": "SZSE",
            "rzye": Decimal("9999.5"),
            "rzmre": None,
            "rzche": None,
            "rzrqye": Decimal("9999.5"),
        },
    ]


def test_margin_cleaner_raises_for_row_without_required_exchange_id() -> None:
    cleaner = MarginDailyCleaner()

    with pytest.raises(ValueError, match="missing required field: exchange_id"):
        cleaner.clean([{"trade_date": "20240102"}])


def test_margin_pipeline_plans_trade_day_chunks_of_300() -> None:
    start = date(2024, 1, 2)
    trade_days = {start + timedelta(days=offset) for offset in range(301)}
    ordered_trade_days = sorted(trade_days)
    calendar = TradingCalendarService(store=FakeCalendarStore(trade_days), syncer=FakeSyncer())
    pipeline = MarginDailyPipeline(
        calendar=calendar,
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
    )

    chunks = pipeline.plan_chunks(
        {
            "params": {
                "start_date": ordered_trade_days[0].isoformat(),
                "end_date": ordered_trade_days[-1].isoformat(),
            }
        }
    )

    assert chunks == [
        {
            "params": {
                "start_date": ordered_trade_days[0].isoformat(),
                "end_date": ordered_trade_days[299].isoformat(),
            }
        },
        {
            "params": {
                "start_date": ordered_trade_days[300].isoformat(),
                "end_date": ordered_trade_days[300].isoformat(),
            }
        },
    ]


def test_margin_daily_table_primary_key_and_upsert_keys() -> None:
    pk_columns = [column.name for column in margin_daily.primary_key.columns]
    assert pk_columns == ["trade_date", "exchange_id"]

    strategy = PostgresUpsertStrategy()
    stmt = strategy.build_upsert(
        margin_daily,
        [{"trade_date": date(2024, 1, 2), "exchange_id": "SSE", "rzye": 10000.0}],
        ["trade_date", "exchange_id"],
    )

    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT (trade_date, exchange_id) DO UPDATE" in sql
    assert "rzye = excluded.rzye" in sql
