from __future__ import annotations

import inspect
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine

import api.main as api_main
from core.calendar.service import TradingCalendarService
from core.clean.market_daily_info_cleaner import (
    _SZ_CATEGORY_TO_TARGET,
    MarketDailyInfoCleaner,
)
from core.fetch.retry import RetryPolicy
from infra.db.repository import PostgresUpsertStrategy
from infra.db.tables import market_daily_info
from infra.fetcher.tushare_market_daily_info_fetcher import TushareMarketDailyInfoFetcher
from infra.fetcher.tushare_sz_daily_info_fetcher import TushareSzDailyInfoFetcher
from infra.tushare.client import TushareProClient
from services.pipelines.market_daily_info_pipeline import (
    MarketDailyInfoPipeline,
    _parse_date_param,
)

_DEFAULT_FIELDS = (
    "trade_date,ts_code,ts_name,com_count,total_share,float_share,total_mv,"
    "float_mv,amount,vol,trans_count,pe,tr,exchange"
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
    daily_info_calls: list[tuple[str, str, str, str | None]] = field(default_factory=list)
    sz_daily_info_calls: list[tuple[str, str]] = field(default_factory=list)
    daily_info_rows: list[dict[str, Any]] = field(default_factory=list)
    sz_daily_info_rows: list[dict[str, Any]] = field(default_factory=list)

    def daily_info(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        exchange: str | None = None,
    ) -> list[dict[str, object]]:
        self.daily_info_calls.append((start_date, end_date, fields, exchange))
        return list(self.daily_info_rows)

    def sz_daily_info(self, start_date: str, end_date: str) -> list[dict[str, object]]:
        self.sz_daily_info_calls.append((start_date, end_date))
        return list(self.sz_daily_info_rows)


def _row(
    trade_date: str = "20240102",
    ts_code: str = "000001.SZ",
    ts_name: str = "平安银行",
    com_count: object = 1,
    total_share: object = 1940591.8,
    float_share: object = 1940576.9,
    total_mv: object = 3000000.0,
    float_mv: object = 2900000.0,
    amount: object = 500000.0,
    vol: object = 60000.0,
    trans_count: object = 80000.0,
    pe: object = 6.5,
    tr: object = 0.35,
    exchange: str | None = "SH",
) -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "ts_code": ts_code,
        "ts_name": ts_name,
        "com_count": com_count,
        "total_share": total_share,
        "float_share": float_share,
        "total_mv": total_mv,
        "float_mv": float_mv,
        "amount": amount,
        "vol": vol,
        "trans_count": trans_count,
        "pe": pe,
        "tr": tr,
        "exchange": exchange,
    }


def _sz_row(
    ts_code: str = "主板A股",
    trade_date: str = "20240102",
    count: object = 2712,
    amount: object = 100000000.0,
    vol: object = 200000000.0,
    total_share: object = 3000000000.0,
    total_mv: object = 400000000000.0,
    float_share: object = 5000000000.0,
    float_mv: object = 600000000000.0,
) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "count": count,
        "amount": amount,
        "vol": vol,
        "total_share": total_share,
        "total_mv": total_mv,
        "float_share": float_share,
        "float_mv": float_mv,
    }


def _calendar(dates: set[date]) -> TradingCalendarService:
    return TradingCalendarService(store=FakeCalendarStore(dates), syncer=FakeSyncer())


def test_tushare_pro_client_daily_info_accepts_optional_exchange() -> None:
    client = TushareProClient(token="dummy-token")
    parameters = inspect.signature(client.daily_info).parameters
    assert list(parameters) == ["start_date", "end_date", "fields", "exchange"]
    assert parameters["exchange"].default is None


def test_tushare_pro_client_exposes_sz_daily_info_without_fields_or_exchange() -> None:
    client = TushareProClient(token="dummy-token")
    parameters = inspect.signature(client.sz_daily_info).parameters
    assert list(parameters) == ["start_date", "end_date"]


def test_market_daily_info_fetcher_requests_sh_exchange_and_default_fields() -> None:
    client = FakeTushareClient(daily_info_rows=[_row()])
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == [{**_row(), "_source": "sh"}]
    assert client.daily_info_calls == [("20240102", "20240105", _DEFAULT_FIELDS, "SH")]


def test_market_daily_info_fetcher_accepts_compact_dates_and_custom_fields() -> None:
    client = FakeTushareClient(daily_info_rows=[_row()])
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    fetcher.fetch(
        {
            "params": {
                "start_date": "20240102",
                "end_date": "20240105",
                "fields": "trade_date,ts_code,pe,tr",
            }
        }
    )

    assert client.daily_info_calls == [("20240102", "20240105", "trade_date,ts_code,pe,tr", "SH")]


def test_market_daily_info_fetcher_returns_empty_for_empty_response() -> None:
    client = FakeTushareClient(daily_info_rows=[])
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == []
    assert client.daily_info_calls == [("20240102", "20240105", _DEFAULT_FIELDS, "SH")]


def test_market_daily_info_fetcher_drops_non_sh_rows_defensively() -> None:
    client = FakeTushareClient(
        daily_info_rows=[
            _row(exchange="SH"),
            _row(ts_code="000002.SZ", exchange="SZ"),
            _row(ts_code="000003.SZ", exchange=None),
        ]
    )
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == [{**_row(exchange="SH"), "_source": "sh"}]
    assert client.daily_info_calls == [("20240102", "20240105", _DEFAULT_FIELDS, "SH")]


def test_market_daily_info_fetcher_rejects_missing_start_date() -> None:
    client = FakeTushareClient(daily_info_rows=[_row()])
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="missing required param: start_date"):
        fetcher.fetch(
            {
                "params": {
                    "end_date": "2024-01-05",
                }
            }
        )
    assert client.daily_info_calls == []


def test_market_daily_info_fetcher_rejects_inverted_date_range() -> None:
    client = FakeTushareClient(daily_info_rows=[_row()])
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="start_date must be on or before end_date"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-05",
                    "end_date": "2024-01-02",
                }
            }
        )
    assert client.daily_info_calls == []


def test_market_daily_info_fetcher_rejects_malformed_dates() -> None:
    client = FakeTushareClient(daily_info_rows=[_row()])
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="dates must be in YYYYMMDD format"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024/01/02",
                    "end_date": "2024-01-05",
                }
            }
        )
    assert client.daily_info_calls == []


def test_market_daily_info_fetcher_rejects_response_beyond_api_row_limit() -> None:
    rows = [_row() for _ in range(4001)]
    client = FakeTushareClient(daily_info_rows=rows)
    fetcher = TushareMarketDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="exceeded per-request row limit"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-05",
                }
            }
        )


def test_sz_daily_info_fetcher_calls_without_fields_or_exchange() -> None:
    client = FakeTushareClient(sz_daily_info_rows=[_sz_row()])
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == [{**_sz_row(), "_source": "sz"}]
    assert client.sz_daily_info_calls == [("20240102", "20240105")]


def test_sz_daily_info_fetcher_accepts_compact_dates() -> None:
    client = FakeTushareClient(sz_daily_info_rows=[_sz_row()])
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    fetcher.fetch(
        {
            "params": {
                "start_date": "20240102",
                "end_date": "20240105",
            }
        }
    )

    assert client.sz_daily_info_calls == [("20240102", "20240105")]


def test_sz_daily_info_fetcher_returns_empty_for_empty_response() -> None:
    client = FakeTushareClient(sz_daily_info_rows=[])
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == []
    assert client.sz_daily_info_calls == [("20240102", "20240105")]


def test_sz_daily_info_fetcher_rejects_missing_end_date() -> None:
    client = FakeTushareClient(sz_daily_info_rows=[_sz_row()])
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="missing required param: end_date"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-02",
                }
            }
        )
    assert client.sz_daily_info_calls == []


def test_sz_daily_info_fetcher_rejects_inverted_date_range() -> None:
    client = FakeTushareClient(sz_daily_info_rows=[_sz_row()])
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="start_date must be on or before end_date"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-05",
                    "end_date": "2024-01-02",
                }
            }
        )
    assert client.sz_daily_info_calls == []


def test_sz_daily_info_fetcher_rejects_malformed_dates() -> None:
    client = FakeTushareClient(sz_daily_info_rows=[_sz_row()])
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="dates must be in YYYYMMDD format"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024/01/02",
                    "end_date": "2024-01-05",
                }
            }
        )
    assert client.sz_daily_info_calls == []


def test_sz_daily_info_fetcher_rejects_response_beyond_api_row_limit() -> None:
    rows = [_sz_row() for _ in range(2001)]
    client = FakeTushareClient(sz_daily_info_rows=rows)
    fetcher = TushareSzDailyInfoFetcher(client=client, retry_policy=RetryPolicy())

    with pytest.raises(ValueError, match="exceeded per-request row limit"):
        fetcher.fetch(
            {
                "params": {
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-05",
                }
            }
        )


def test_market_daily_info_cleaner_normalizes_sh_fields_and_strips_source_tag() -> None:
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean(
        [
            {
                **_row(
                    trade_date="2024-01-02",
                    tr=None,
                    com_count=None,
                    trans_count=123456.5,
                ),
                "_source": "sh",
            },
            {
                "trade_date": "20240103",
                "ts_code": "600000.SH",
                "ts_name": None,
                "com_count": 2,
                "trans_count": 100,
                "exchange": "SH",
            },
        ]
    )

    assert cleaned == [
        {
            "trade_date": date(2024, 1, 2),
            "ts_code": "000001.SZ",
            "ts_name": "平安银行",
            "com_count": None,
            "total_share": Decimal("1940591.8"),
            "float_share": Decimal("1940576.9"),
            "total_mv": Decimal("3000000.0"),
            "float_mv": Decimal("2900000.0"),
            "amount": Decimal("500000.0"),
            "vol": Decimal("60000.0"),
            "trans_count": Decimal("123456.5"),
            "pe": Decimal("6.5"),
            "tr": None,
            "exchange": "SH",
        },
        {
            "trade_date": date(2024, 1, 3),
            "ts_code": "600000.SH",
            "ts_name": None,
            "com_count": 2,
            "trans_count": Decimal("100"),
            "exchange": "SH",
        },
    ]


def test_market_daily_info_cleaner_maps_and_scales_sz_rows() -> None:
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean(
        [
            {
                **_sz_row(
                    amount=123456789.5,
                    vol=987654321.5,
                    total_share=12345678901.5,
                    total_mv=987654321098.5,
                    float_share=5555555555.5,
                    float_mv=666666666666.5,
                ),
                "_source": "sz",
            }
        ]
    )

    assert cleaned == [
        {
            "trade_date": date(2024, 1, 2),
            "ts_code": "SZ_A",
            "ts_name": "深圳主板A股",
            "com_count": 2712,
            "total_share": Decimal("123.456789015"),
            "float_share": Decimal("55.555555555"),
            "total_mv": Decimal("9876.543210985"),
            "float_mv": Decimal("6666.666666665"),
            "amount": Decimal("1.234567895"),
            "vol": Decimal("9.876543215"),
            "trans_count": None,
            "pe": None,
            "tr": None,
            "exchange": "SZ",
        }
    ]


def test_market_daily_info_cleaner_sz_row_preserves_null_and_nan_as_null() -> None:
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean(
        [
            {
                "_source": "sz",
                "ts_code": "ETF",
                "trade_date": "20240103",
                "count": None,
                "amount": float("nan"),
                "vol": None,
                "total_share": float("nan"),
                "total_mv": None,
                "float_share": None,
                "float_mv": None,
            }
        ]
    )

    assert cleaned == [
        {
            "trade_date": date(2024, 1, 3),
            "ts_code": "SZ_FUND_ETF",
            "ts_name": "深圳基金ETF",
            "com_count": None,
            "total_share": None,
            "float_share": None,
            "total_mv": None,
            "float_mv": None,
            "amount": None,
            "vol": None,
            "trans_count": None,
            "pe": None,
            "tr": None,
            "exchange": "SZ",
        }
    ]


def test_market_daily_info_cleaner_covers_all_sz_categories() -> None:
    # Exhaustive 2008-01-02..2025-12-31 source scan found exactly 23 categories.
    assert len(_SZ_CATEGORY_TO_TARGET) == 23
    cleaner = MarketDailyInfoCleaner()

    for category, (target_ts_code, target_ts_name) in _SZ_CATEGORY_TO_TARGET.items():
        cleaned = cleaner.clean([{"_source": "sz", "ts_code": category, "trade_date": "20240102"}])
        assert cleaned == [
            {
                "trade_date": date(2024, 1, 2),
                "ts_code": target_ts_code,
                "ts_name": target_ts_name,
                "com_count": None,
                "total_share": None,
                "float_share": None,
                "total_mv": None,
                "float_mv": None,
                "amount": None,
                "vol": None,
                "trans_count": None,
                "pe": None,
                "tr": None,
                "exchange": "SZ",
            }
        ]


def test_market_daily_info_cleaner_deduplicates_identical_warrant_labels() -> None:
    """权证 and 股票权证 are duplicate source labels with identical values;
    both map to (trade_date, SZ_WR), so clean() must yield a single row."""
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean(
        [
            {**_sz_row(ts_code="权证"), "_source": "sz"},
            {**_sz_row(ts_code="股票权证"), "_source": "sz"},
        ]
    )

    assert len(cleaned) == 1
    assert cleaned[0]["trade_date"] == date(2024, 1, 2)
    assert cleaned[0]["ts_code"] == "SZ_WR"
    assert cleaned[0]["ts_name"] == "深圳权证"
    assert cleaned[0]["exchange"] == "SZ"
    assert cleaned[0]["com_count"] == 2712
    assert cleaned[0]["amount"] == Decimal("1.000000000")


def test_market_daily_info_cleaner_conflicting_duplicate_target_raises() -> None:
    """A second normalized record with the same (trade_date, ts_code) but
    different values must raise instead of silently last-write-wins."""
    cleaner = MarketDailyInfoCleaner()

    with pytest.raises(ValueError, match="conflicting normalized records"):
        cleaner.clean(
            [
                {**_sz_row(ts_code="权证"), "_source": "sz"},
                {**_sz_row(ts_code="股票权证", amount=999999999.0), "_source": "sz"},
            ]
        )


def test_market_daily_info_cleaner_dedup_preserves_output_order() -> None:
    """De-duplication keeps the first occurrence at its original position."""
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean(
        [
            {**_sz_row(ts_code="权证", trade_date="20240102"), "_source": "sz"},
            {**_sz_row(ts_code="股票", trade_date="20240102"), "_source": "sz"},
            {**_sz_row(ts_code="股票权证", trade_date="20240102"), "_source": "sz"},
        ]
    )

    assert [record["ts_code"] for record in cleaned] == ["SZ_WR", "SZ_MARKET"]


def test_market_daily_info_cleaner_deduplicates_identical_sh_rows() -> None:
    """De-duplication applies to all normalized rows, not only SZ warrants."""
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean([_row(), _row()])

    assert cleaned == [
        {
            "trade_date": date(2024, 1, 2),
            "ts_code": "000001.SZ",
            "ts_name": "平安银行",
            "com_count": 1,
            "total_share": Decimal("1940591.8"),
            "float_share": Decimal("1940576.9"),
            "total_mv": Decimal("3000000.0"),
            "float_mv": Decimal("2900000.0"),
            "amount": Decimal("500000.0"),
            "vol": Decimal("60000.0"),
            "trans_count": Decimal("80000.0"),
            "pe": Decimal("6.5"),
            "tr": Decimal("0.35"),
            "exchange": "SH",
        }
    ]


def test_market_daily_info_cleaner_sz_row_unknown_category_fails_loudly() -> None:
    cleaner = MarketDailyInfoCleaner()

    with pytest.raises(ValueError, match="unknown sz_daily_info category"):
        cleaner.clean([{**_sz_row(ts_code="北交所"), "_source": "sz"}])


def test_market_daily_info_cleaner_sz_row_missing_category_fails_loudly() -> None:
    cleaner = MarketDailyInfoCleaner()

    with pytest.raises(ValueError, match="unknown sz_daily_info category"):
        cleaner.clean([{"_source": "sz", "trade_date": "20240102"}])


def test_market_daily_info_cleaner_rejects_missing_required_fields() -> None:
    cleaner = MarketDailyInfoCleaner()

    with pytest.raises(ValueError, match="missing required field: ts_code"):
        cleaner.clean([{"trade_date": "20240102"}])

    with pytest.raises(ValueError, match="missing required field: trade_date"):
        cleaner.clean([{"ts_code": "000001.SZ"}])


def test_market_daily_info_cleaner_sh_row_nan_com_count_becomes_null() -> None:
    """Real SH daily_info rows (e.g. SH_FUND_* boards) carry NaN com_count;
    it must normalize to NULL instead of failing the chunk."""
    cleaner = MarketDailyInfoCleaner()

    cleaned = cleaner.clean(
        [
            {
                **_row(
                    ts_code="SH_FUND_ETF",
                    ts_name="上海基金ETF",
                    com_count=float("nan"),
                    trans_count=float("nan"),
                    tr=None,
                ),
                "_source": "sh",
            }
        ]
    )

    assert cleaned == [
        {
            "trade_date": date(2024, 1, 2),
            "ts_code": "SH_FUND_ETF",
            "ts_name": "上海基金ETF",
            "com_count": None,
            "total_share": Decimal("1940591.8"),
            "float_share": Decimal("1940576.9"),
            "total_mv": Decimal("3000000.0"),
            "float_mv": Decimal("2900000.0"),
            "amount": Decimal("500000.0"),
            "vol": Decimal("60000.0"),
            "trans_count": None,
            "pe": Decimal("6.5"),
            "tr": None,
            "exchange": "SH",
        }
    ]


def test_market_daily_info_cleaner_rejects_non_integral_com_count() -> None:
    cleaner = MarketDailyInfoCleaner()

    with pytest.raises(ValueError, match="expected integer-like value"):
        cleaner.clean([_row(com_count=1.5)])


def test_market_daily_info_pipeline_plans_sh_then_sz_50_day_chunks() -> None:
    start = date(2024, 1, 2)
    trade_days = {start + timedelta(days=offset) for offset in range(101)}
    ordered_trade_days = sorted(trade_days)
    pipeline = MarketDailyInfoPipeline(
        calendar=_calendar(trade_days),
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

    assert [chunk["params"]["source"] for chunk in chunks] == [
        "sh",
        "sh",
        "sh",
        "sz",
        "sz",
        "sz",
    ]
    expected_ranges = [
        (ordered_trade_days[0], ordered_trade_days[49]),
        (ordered_trade_days[50], ordered_trade_days[99]),
        (ordered_trade_days[100], ordered_trade_days[100]),
    ]
    sh_chunks = [chunk for chunk in chunks if chunk["params"]["source"] == "sh"]
    sz_chunks = [chunk for chunk in chunks if chunk["params"]["source"] == "sz"]
    for source_chunks in (sh_chunks, sz_chunks):
        for chunk, (chunk_start, chunk_end) in zip(source_chunks, expected_ranges):
            assert chunk["params"]["start_date"] == chunk_start.isoformat()
            assert chunk["params"]["end_date"] == chunk_end.isoformat()
    assert sh_chunks[0]["params"] == {
        "start_date": ordered_trade_days[0].isoformat(),
        "end_date": ordered_trade_days[49].isoformat(),
        "source": "sh",
    }
    assert sz_chunks[0]["params"] == {
        "start_date": ordered_trade_days[0].isoformat(),
        "end_date": ordered_trade_days[49].isoformat(),
        "source": "sz",
    }


def test_market_daily_info_pipeline_parse_date_param_accepts_both_formats_and_dates() -> None:
    assert _parse_date_param("2024-01-02") == date(2024, 1, 2)
    assert _parse_date_param("20240102") == date(2024, 1, 2)
    assert _parse_date_param(date(2024, 1, 2)) == date(2024, 1, 2)
    with pytest.raises(ValueError):
        _parse_date_param("2024/01/02")
    with pytest.raises(ValueError, match="expected date or YYYY-MM-DD or YYYYMMDD"):
        _parse_date_param(123)


def test_market_daily_info_pipeline_plans_compact_dates_to_iso_chunks() -> None:
    """Compact YYYYMMDD input dates must be normalized to ISO YYYY-MM-DD chunk
    boundaries for both SH and SZ chunks (the shared chunk policy only accepts
    YYYY-MM-DD)."""
    start = date(2024, 1, 2)
    trade_days = {start + timedelta(days=offset) for offset in range(101)}
    ordered_trade_days = sorted(trade_days)
    pipeline = MarketDailyInfoPipeline(
        calendar=_calendar(trade_days),
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
    )

    chunks = pipeline.plan_chunks(
        {
            "params": {
                "start_date": ordered_trade_days[0].strftime("%Y%m%d"),
                "end_date": ordered_trade_days[-1].strftime("%Y%m%d"),
            }
        }
    )

    assert [chunk["params"]["source"] for chunk in chunks] == [
        "sh",
        "sh",
        "sh",
        "sz",
        "sz",
        "sz",
    ]
    expected_ranges = [
        (ordered_trade_days[0], ordered_trade_days[49]),
        (ordered_trade_days[50], ordered_trade_days[99]),
        (ordered_trade_days[100], ordered_trade_days[100]),
    ]
    for source_chunks in (
        [chunk for chunk in chunks if chunk["params"]["source"] == "sh"],
        [chunk for chunk in chunks if chunk["params"]["source"] == "sz"],
    ):
        for chunk, (chunk_start, chunk_end) in zip(source_chunks, expected_ranges):
            assert chunk["params"]["start_date"] == chunk_start.isoformat()
            assert chunk["params"]["end_date"] == chunk_end.isoformat()


def test_market_daily_info_pipeline_plans_sz_chunks_only_from_2008() -> None:
    start = date(2007, 6, 1)
    trade_days = {start + timedelta(days=offset) for offset in range(300)}
    ordered_trade_days = sorted(trade_days)
    pipeline = MarketDailyInfoPipeline(
        calendar=_calendar(trade_days),
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

    sh_chunks = [chunk for chunk in chunks if chunk["params"]["source"] == "sh"]
    sz_chunks = [chunk for chunk in chunks if chunk["params"]["source"] == "sz"]
    assert sh_chunks and sz_chunks
    assert sh_chunks[0]["params"]["start_date"] == ordered_trade_days[0].isoformat()
    assert sh_chunks[-1]["params"]["end_date"] == ordered_trade_days[-1].isoformat()
    assert sz_chunks[0]["params"]["start_date"] == "2008-01-02"
    for chunk in sz_chunks:
        assert chunk["params"]["start_date"] >= "2008-01-02"


def test_market_daily_info_pipeline_plans_no_sz_chunks_before_source_start() -> None:
    start = date(2007, 1, 2)
    trade_days = {start + timedelta(days=offset) for offset in range(250)}
    ordered_trade_days = sorted(trade_days)
    pipeline = MarketDailyInfoPipeline(
        calendar=_calendar(trade_days),
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

    assert chunks
    assert all(chunk["params"]["source"] == "sh" for chunk in chunks)


def test_market_daily_info_pipeline_routes_fetch_by_source() -> None:
    client = FakeTushareClient(
        daily_info_rows=[_row()],
        sz_daily_info_rows=[_sz_row()],
    )
    pipeline = MarketDailyInfoPipeline(
        calendar=_calendar(set()),
        client=client,
        retry_policy=RetryPolicy(),
    )

    sh_rows = pipeline.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
                "source": "sh",
            }
        }
    )
    sz_rows = pipeline.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
                "source": "sz",
            }
        }
    )

    assert sh_rows == [{**_row(), "_source": "sh"}]
    assert sz_rows == [{**_sz_row(), "_source": "sz"}]
    assert client.daily_info_calls == [("20240102", "20240105", _DEFAULT_FIELDS, "SH")]
    assert client.sz_daily_info_calls == [("20240102", "20240105")]


def test_market_daily_info_table_primary_key_and_upsert_keys() -> None:
    pk_columns = [column.name for column in market_daily_info.primary_key.columns]
    assert pk_columns == ["trade_date", "ts_code"]

    strategy = PostgresUpsertStrategy()
    stmt = strategy.build_upsert(
        market_daily_info,
        [{"trade_date": date(2024, 1, 2), "ts_code": "SZ_A", "com_count": 2712}],
        ["trade_date", "ts_code"],
    )

    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT (trade_date, ts_code) DO UPDATE" in sql
    assert "com_count = excluded.com_count" in sql


def test_market_daily_info_create_app_wiring_runs_task(
    monkeypatch,
    postgres_engine: Engine,
) -> None:
    trade_days = {date(2024, 1, 2), date(2024, 1, 3)}
    calendar = _calendar(trade_days)
    client = FakeTushareClient(
        daily_info_rows=[
            _row(trade_date="20240102", ts_code="000001.SZ"),
            _row(
                trade_date="20240102",
                ts_code="600000.SH",
                ts_name="浦发银行",
                tr=None,
            ),
            _row(
                trade_date="20240103",
                ts_code="000001.SZ",
                com_count=None,
                trans_count=90000.5,
                tr=None,
            ),
        ],
        sz_daily_info_rows=[
            _sz_row(trade_date="20240102"),
            _sz_row(
                ts_code="ETF",
                trade_date="20240103",
                count=None,
                amount=float("nan"),
                vol=None,
                total_share=None,
                total_mv=None,
                float_share=None,
                float_mv=None,
            ),
        ],
    )

    monkeypatch.setattr(api_main, "create_engine_from_config", lambda config: postgres_engine)
    monkeypatch.setattr(
        api_main,
        "build_calendar_service",
        lambda config: SimpleNamespace(calendar=calendar),
    )
    monkeypatch.setattr(api_main, "TushareProClient", lambda token: client)

    with TestClient(api_main.create_app()) as app_client:
        response = app_client.post(
            "/tasks/start",
            json={
                "spec": "get_market_daily_info",
                "pipeline_id": "market_daily_info",
                "source": "unit-test",
                "task_type": "market_daily_info",
                "arguments": {"params": {"start_date": "2024-01-02", "end_date": "2024-01-03"}},
                "options": {},
            },
        )
        assert response.status_code == 200
        task_id = response.json()["task_id"]

        deadline = time.time() + 5
        while time.time() < deadline:
            status = app_client.get(f"/tasks/{task_id}")
            assert status.status_code == 200
            payload = status.json()
            if payload["state"] == "SUCCEEDED":
                break
            if payload["state"] == "FAILED":
                raise AssertionError(payload["error"])
            time.sleep(0.1)
        else:
            raise AssertionError("market_daily_info app task did not complete")

    with postgres_engine.begin() as connection:
        rows = connection.execute(
            select(
                market_daily_info.c.trade_date,
                market_daily_info.c.ts_code,
                market_daily_info.c.ts_name,
                market_daily_info.c.com_count,
                market_daily_info.c.total_share,
                market_daily_info.c.total_mv,
                market_daily_info.c.amount,
                market_daily_info.c.trans_count,
                market_daily_info.c.pe,
                market_daily_info.c.tr,
                market_daily_info.c.exchange,
            ).order_by(market_daily_info.c.trade_date, market_daily_info.c.ts_code)
        ).all()

    assert client.daily_info_calls == [("20240102", "20240103", _DEFAULT_FIELDS, "SH")]
    assert client.sz_daily_info_calls == [("20240102", "20240103")]
    assert rows == [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "平安银行",
            1,
            Decimal("1940591.8000"),
            Decimal("3000000.0000"),
            Decimal("500000.0000"),
            Decimal("80000.0000"),
            Decimal("6.5000"),
            Decimal("0.3500"),
            "SH",
        ),
        (
            date(2024, 1, 2),
            "600000.SH",
            "浦发银行",
            1,
            Decimal("1940591.8000"),
            Decimal("3000000.0000"),
            Decimal("500000.0000"),
            Decimal("80000.0000"),
            Decimal("6.5000"),
            None,
            "SH",
        ),
        (
            date(2024, 1, 2),
            "SZ_A",
            "深圳主板A股",
            2712,
            Decimal("30.0000"),
            Decimal("4000.0000"),
            Decimal("1.0000"),
            None,
            None,
            None,
            "SZ",
        ),
        (
            date(2024, 1, 3),
            "000001.SZ",
            "平安银行",
            None,
            Decimal("1940591.8000"),
            Decimal("3000000.0000"),
            Decimal("500000.0000"),
            Decimal("90000.5000"),
            Decimal("6.5000"),
            None,
            "SH",
        ),
        (
            date(2024, 1, 3),
            "SZ_FUND_ETF",
            "深圳基金ETF",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "SZ",
        ),
    ]
