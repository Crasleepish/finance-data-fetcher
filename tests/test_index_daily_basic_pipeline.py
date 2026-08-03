from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from core.calendar.service import TradingCalendarService
from core.clean.index_daily_basic_cleaner import IndexDailyBasicCleaner
from core.fetch.retry import RetryPolicy
from infra.db.repository import PostgresUpsertStrategy
from infra.db.tables import index_daily_basic
from infra.fetcher.tushare_index_daily_basic_fetcher import TushareIndexDailyBasicFetcher
from services.pipelines.index_daily_basic_pipeline import (
    INDEX_DAILY_BASIC_SOURCE_CODES,
    IndexDailyBasicPipeline,
)

_DEFAULT_FIELDS = (
    "ts_code,trade_date,total_mv,float_mv,total_share,float_share,free_share,"
    "turnover_rate,turnover_rate_f,pe,pe_ttm,pb"
)


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
    calls: list[tuple[str, str, str, str]] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)

    def index_dailybasic(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, object]]:
        self.calls.append((ts_code, start_date, end_date, fields))
        return list(self.rows)


def _row(
    ts_code: str = "000001.SH",
    trade_date: str = "20240102",
    total_mv: object = 4600000.0,
    float_mv: object = 3800000.0,
    total_share: object = 350000.0,
    float_share: object = 300000.0,
    free_share: object = 250000.0,
    turnover_rate: object = 0.5,
    turnover_rate_f: object = 0.6,
    pe: object = 12.5,
    pe_ttm: object = 11.8,
    pb: object = 1.3,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "total_mv": total_mv,
        "float_mv": float_mv,
        "total_share": total_share,
        "float_share": float_share,
        "free_share": free_share,
        "turnover_rate": turnover_rate,
        "turnover_rate_f": turnover_rate_f,
        "pe": pe,
        "pe_ttm": pe_ttm,
        "pb": pb,
    }


def test_index_daily_basic_fetcher_uses_code_date_range_and_default_fields() -> None:
    client = FakeTushareClient(rows=[_row()])
    fetcher = TushareIndexDailyBasicFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "ts_code": "000001.SH",
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == [_row()]
    assert client.calls == [("000001.SH", "20240102", "20240105", _DEFAULT_FIELDS)]


def test_index_daily_basic_fetcher_accepts_compact_dates_and_custom_fields() -> None:
    client = FakeTushareClient(rows=[_row()])
    fetcher = TushareIndexDailyBasicFetcher(client=client, retry_policy=RetryPolicy())

    fetcher.fetch(
        {
            "params": {
                "ts_code": "399006.SZ",
                "start_date": "20240102",
                "end_date": "20240105",
                "fields": "ts_code,trade_date,pe,pb",
            }
        }
    )

    assert client.calls == [("399006.SZ", "20240102", "20240105", "ts_code,trade_date,pe,pb")]


def test_index_daily_basic_fetcher_returns_empty_for_empty_response() -> None:
    client = FakeTushareClient(rows=[])
    fetcher = TushareIndexDailyBasicFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "ts_code": "000905.SH",
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == []
    assert client.calls == [("000905.SH", "20240102", "20240105", _DEFAULT_FIELDS)]


def test_index_daily_basic_fetcher_rejects_missing_ts_code() -> None:
    client = FakeTushareClient(rows=[_row()])
    fetcher = TushareIndexDailyBasicFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="missing required param: ts_code"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-05",
                }
            }
        )
    assert client.calls == []


def test_index_daily_basic_fetcher_rejects_inverted_date_range() -> None:
    client = FakeTushareClient(rows=[_row()])
    fetcher = TushareIndexDailyBasicFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="start_date must be on or before end_date"):
        fetcher.fetch(
            {
                "params": {
                    "ts_code": "000001.SH",
                    "start_date": "2024-01-05",
                    "end_date": "2024-01-02",
                }
            }
        )
    assert client.calls == []


def test_index_daily_basic_fetcher_rejects_response_beyond_api_row_limit() -> None:
    rows = [_row() for _ in range(3001)]
    client = FakeTushareClient(rows=rows)
    fetcher = TushareIndexDailyBasicFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="exceeded per-request row limit"):
        fetcher.fetch(
            {
                "params": {
                    "ts_code": "000001.SH",
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-05",
                }
            }
        )


def test_index_daily_basic_cleaner_normalizes_fields_and_missing_values() -> None:
    cleaner = IndexDailyBasicCleaner()

    cleaned = cleaner.clean(
        [
            _row(trade_date="2024-01-02", pe=None, pe_ttm=None, pb=None),
            {
                "ts_code": "399006.SZ",
                "trade_date": "20240103",
                "total_mv": 9999.5,
                "turnover_rate": None,
            },
        ]
    )

    assert cleaned == [
        {
            "ts_code": "000001.SH",
            "trade_date": date(2024, 1, 2),
            "total_mv": Decimal("4600000.0"),
            "float_mv": Decimal("3800000.0"),
            "total_share": Decimal("350000.0"),
            "float_share": Decimal("300000.0"),
            "free_share": Decimal("250000.0"),
            "turnover_rate": Decimal("0.5"),
            "turnover_rate_f": Decimal("0.6"),
            "pe": None,
            "pe_ttm": None,
            "pb": None,
        },
        {
            "ts_code": "399006.SZ",
            "trade_date": date(2024, 1, 3),
            "total_mv": Decimal("9999.5"),
            "turnover_rate": None,
        },
    ]


def test_index_daily_basic_cleaner_rejects_missing_ts_code() -> None:
    cleaner = IndexDailyBasicCleaner()

    with pytest.raises(ValueError, match="missing required field: ts_code"):
        cleaner.clean([{"trade_date": "20240102"}])


def test_index_daily_basic_cleaner_rejects_missing_trade_date() -> None:
    cleaner = IndexDailyBasicCleaner()

    with pytest.raises(ValueError, match="missing required field: trade_date"):
        cleaner.clean([{"ts_code": "000001.SH"}])


def test_index_daily_basic_cleaner_rejects_invalid_trade_date() -> None:
    cleaner = IndexDailyBasicCleaner()

    with pytest.raises(ValueError, match="time data 'not-a-date' does not match format"):
        cleaner.clean([{"ts_code": "000001.SH", "trade_date": "not-a-date"}])


def test_index_daily_basic_pipeline_plans_300_day_chunks_per_source_code() -> None:
    start = date(2024, 1, 2)
    trade_days = {start + timedelta(days=offset) for offset in range(301)}
    ordered_trade_days = sorted(trade_days)
    calendar = TradingCalendarService(store=FakeCalendarStore(trade_days), syncer=FakeSyncer())
    pipeline = IndexDailyBasicPipeline(
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

    assert len(chunks) == len(INDEX_DAILY_BASIC_SOURCE_CODES) * 2
    first_code_chunks = [
        chunk["params"] for chunk in chunks if chunk["params"]["ts_code"] == "000001.SH"
    ]
    assert first_code_chunks == [
        {
            "ts_code": "000001.SH",
            "start_date": ordered_trade_days[0].isoformat(),
            "end_date": ordered_trade_days[299].isoformat(),
        },
        {
            "ts_code": "000001.SH",
            "start_date": ordered_trade_days[300].isoformat(),
            "end_date": ordered_trade_days[300].isoformat(),
        },
    ]
    for chunk in chunks:
        assert chunk["params"]["ts_code"] in INDEX_DAILY_BASIC_SOURCE_CODES
        assert chunk["params"]["start_date"] <= chunk["params"]["end_date"]
    expected_order = [
        code
        for code in INDEX_DAILY_BASIC_SOURCE_CODES
        for _ in range(2)
    ]
    assert [chunk["params"]["ts_code"] for chunk in chunks] == expected_order


def test_index_daily_basic_table_primary_key_and_upsert_keys() -> None:
    pk_columns = [column.name for column in index_daily_basic.primary_key.columns]
    assert pk_columns == ["ts_code", "trade_date"]

    strategy = PostgresUpsertStrategy()
    stmt = strategy.build_upsert(
        index_daily_basic,
        [{"ts_code": "000001.SH", "trade_date": date(2024, 1, 2), "pe": 12.5}],
        ["ts_code", "trade_date"],
    )

    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT (ts_code, trade_date) DO UPDATE" in sql
    assert "pe = excluded.pe" in sql
