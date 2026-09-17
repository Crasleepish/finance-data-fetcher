from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import api.main as api_main
from core.calendar.service import TradingCalendarService
from core.clean.etf_daily_size_cleaner import (
    NAV_SOURCE,
    SEED_SOURCE,
    SHARE_SOURCE,
    EtfDailySizeCleaner,
)
from core.fetch.retry import RetryPolicy
from infra.db.tables import etf_daily_size
from infra.fetcher.tushare_etf_daily_size_fetcher import (
    MAX_SHARE_ROWS,
    NAV_MARKET,
    TushareEtfDailySizeFetcher,
)
from models.task_spec import TaskSpec
from services.pipeline_selector import load_pipeline_mapping
from services.pipelines.etf_daily_size_pipeline import EtfDailySizePipeline

_SHARE_FIELDS = "ts_code,trade_date,fd_share"
_NAV_FIELDS = "ts_code,nav_date,unit_nav,ann_date"


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
        earlier = [candidate for candidate in self.dates if candidate < day]
        return max(earlier) if earlier else None

    def next_trade_day(self, day: date) -> date | None:
        later = [candidate for candidate in self.dates if candidate > day]
        return min(later) if later else None

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
    share_calls: list[tuple[str, str, str]] = field(default_factory=list)
    nav_calls: list[tuple[str, str, str]] = field(default_factory=list)
    share_rows: dict[tuple[str, str], list[dict[str, object]]] = field(default_factory=dict)
    nav_rows: dict[str, list[dict[str, object]]] = field(default_factory=dict)

    def fund_share(self, trade_date: str, market: str, fields: str) -> list[dict[str, object]]:
        self.share_calls.append((trade_date, market, fields))
        return [dict(row) for row in self.share_rows.get((trade_date, market), [])]

    def fund_nav_market(self, nav_date: str, market: str, fields: str) -> list[dict[str, object]]:
        self.nav_calls.append((nav_date, market, fields))
        return [dict(row) for row in self.nav_rows.get(nav_date, [])]


class _PipelineWithoutDbSeeds(EtfDailySizePipeline):
    """Pipeline whose DB seed lookup is stubbed (target table treated as empty)."""

    def _load_db_seeds(self, trade_date: date) -> dict[str, tuple[date, Decimal]]:
        return {}


class _NaTLike:
    """Minimal pandas-NaT-like sentinel: not equal to itself."""

    def __eq__(self, other: object) -> bool:
        return False

    def __ne__(self, other: object) -> bool:
        return True


def _calendar(days: set[date]) -> TradingCalendarService:
    return TradingCalendarService(store=FakeCalendarStore(days), syncer=FakeSyncer())


def _sqlite_engine() -> Engine:
    # Never queried by tests using _PipelineWithoutDbSeeds; avoids Docker entirely.
    return create_engine("sqlite://")


def _share(ts_code: str, trade_date: str, fd_share: object) -> dict[str, object]:
    return {"ts_code": ts_code, "trade_date": trade_date, "fd_share": fd_share}


def _nav(
    ts_code: str, nav_date: str, unit_nav: object, ann_date: object = None
) -> dict[str, object]:
    return {"ts_code": ts_code, "nav_date": nav_date, "unit_nav": unit_nav, "ann_date": ann_date}


def _share_with_source(ts_code: str, trade_date: str, fd_share: object) -> dict[str, object]:
    return {**_share(ts_code, trade_date, fd_share), "_source": SHARE_SOURCE}


def _seed(ts_code: str, trade_date: str, fd_share: object) -> dict[str, object]:
    return {**_share(ts_code, trade_date, fd_share), "_source": SEED_SOURCE}


def _seed_row(ts_code: str, trade_date: date, fd_share: object) -> dict[str, object]:
    return {
        "_source": SEED_SOURCE,
        "_chunk_date": date(2024, 1, 4),
        "ts_code": ts_code,
        "trade_date": trade_date,
        "fd_share": fd_share,
    }


# --- Fetcher: single trade day, SH/SZ/E parameters, truncation guard -------------


def test_fetch_queries_sh_sz_shares_then_e_navs_for_one_trade_day() -> None:
    client = FakeTushareClient(
        share_rows={
            ("20240102", "SH"): [_share("510300.SH", "20240102", 12.5)],
            ("20240102", "SZ"): [_share("159915.SZ", "20240102", 7)],
        },
        nav_rows={"20240102": [_nav("510300.SH", "20240102", 1.1, "20240102")]},
    )
    fetcher = TushareEtfDailySizeFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch({"params": {"trade_date": "2024-01-02"}})

    assert client.share_calls == [
        ("20240102", "SH", _SHARE_FIELDS),
        ("20240102", "SZ", _SHARE_FIELDS),
    ]
    assert client.nav_calls == [("20240102", NAV_MARKET, _NAV_FIELDS)]
    assert [row["_source"] for row in rows] == [SHARE_SOURCE, SHARE_SOURCE, NAV_SOURCE]
    assert [row["ts_code"] for row in rows] == ["510300.SH", "159915.SZ", "510300.SH"]


def test_fetch_shares_rejects_rows_at_request_cap() -> None:
    below_cap = FakeTushareClient(
        share_rows={
            ("20240102", "SH"): [
                _share("510300.SH", "20240102", 1.0) for _ in range(MAX_SHARE_ROWS - 1)
            ]
        }
    )
    fetcher = TushareEtfDailySizeFetcher(client=below_cap, retry_policy=RetryPolicy())
    assert len(fetcher.fetch({"params": {"trade_date": "20240102"}})) == MAX_SHARE_ROWS - 1

    at_cap = FakeTushareClient(
        share_rows={
            ("20240102", "SH"): [
                _share("510300.SH", "20240102", 1.0) for _ in range(MAX_SHARE_ROWS)
            ]
        }
    )
    fetcher = TushareEtfDailySizeFetcher(client=at_cap, retry_policy=RetryPolicy())
    with pytest.raises(RuntimeError, match="2000-row request cap"):
        fetcher.fetch({"params": {"trade_date": "20240102"}})


# --- Cleaner: AUM units in Decimal, universe left join, missing values -----------


def test_clean_computes_decimal_aum_without_scaling_the_10k_column() -> None:
    record = EtfDailySizeCleaner().clean(
        [
            {
                "_source": SHARE_SOURCE,
                "ts_code": "510300.SH",
                "trade_date": "20240102",
                "fd_share": "100",
            },
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": "20240102",
                "unit_nav": "2.5",
            },
        ]
    )[0]

    # fd_share is 万份 and unit_nav is 元/份, so aum_10k_cny is already 万元.
    assert record == {
        "trade_date": date(2024, 1, 2),
        "ts_code": "510300.SH",
        "fd_share": Decimal("100"),
        "share_source_date": date(2024, 1, 2),
        "nav_date": date(2024, 1, 2),
        "ann_date": date(2024, 1, 2),
        "unit_nav": Decimal("2.5"),
        "aum_10k_cny": Decimal("250.0000"),
        "aum_cny": Decimal("2500000.00"),
        "nav_missing": False,
    }


def test_clean_quantizes_aum_10k_and_aum_cny_from_unrounded_raw() -> None:
    record = EtfDailySizeCleaner().clean(
        [
            {
                "_source": SHARE_SOURCE,
                "ts_code": "510300.SH",
                "trade_date": "20240102",
                "fd_share": "1.2345",
            },
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": None,
                "unit_nav": "1.1111",
            },
        ]
    )[0]

    # raw = 1.2345 * 1.1111 = 1.37165295
    assert record["aum_10k_cny"] == Decimal("1.3717")
    # aum_cny quantizes the raw product, not the rounded 万元 value.
    assert record["aum_cny"] == Decimal("13716.53")
    assert record["aum_cny"] != (record["aum_10k_cny"] * Decimal("10000")).quantize(Decimal("0.01"))
    assert record["ann_date"] is None


def test_clean_forward_fills_seed_and_records_share_source_date() -> None:
    records = EtfDailySizeCleaner().clean(
        [
            _share_with_source("510300.SH", "20240102", "1000"),
            _seed("510300.SH", "20231220", "888"),
            _seed("510500.SH", "20240101", "20"),
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": "20240102",
                "unit_nav": "2",
            },
            {
                "_source": NAV_SOURCE,
                "ts_code": "510500.SH",
                "nav_date": "20240102",
                "ann_date": "20240102",
                "unit_nav": "1.5",
            },
        ]
    )

    assert [record["ts_code"] for record in records] == ["510300.SH", "510500.SH"]
    first, second = records
    # Same-day fund_share overrides the seed for 510300.SH.
    assert first["fd_share"] == Decimal("1000")
    assert first["share_source_date"] == date(2024, 1, 2)
    assert first["aum_10k_cny"] == Decimal("2000.0000")
    # 510500.SH has no same-day event, so the stored seed is carried forward.
    assert second["fd_share"] == Decimal("20")
    assert second["share_source_date"] == date(2024, 1, 1)
    assert second["aum_10k_cny"] == Decimal("30.0000")
    assert second["aum_cny"] == Decimal("300000.00")


def test_clean_selects_max_ann_date_regardless_of_row_order() -> None:
    record = EtfDailySizeCleaner().clean(
        [
            _share_with_source("510300.SH", "20240102", "100"),
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": "20240105",
                "unit_nav": "1.2",
            },
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": "20240103",
                "unit_nav": "1.0",
            },
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": None,
                "unit_nav": "0.5",
            },
        ]
    )[0]

    assert record["ann_date"] == date(2024, 1, 5)
    assert record["unit_nav"] == Decimal("1.2")
    assert record["aum_10k_cny"] == Decimal("120.0000")


def test_clean_keeps_missing_nav_instead_of_falling_back_to_older_version() -> None:
    record = EtfDailySizeCleaner().clean(
        [
            _share_with_source("510300.SH", "20240102", "100"),
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": "20240102",
                "unit_nav": "1.5",
            },
            {
                "_source": NAV_SOURCE,
                "ts_code": "510300.SH",
                "nav_date": "20240102",
                "ann_date": "20240105",
                "unit_nav": None,
            },
        ]
    )[0]

    assert record["ann_date"] == date(2024, 1, 5)
    assert record["unit_nav"] is None
    assert record["aum_10k_cny"] is None
    assert record["aum_cny"] is None
    assert record["nav_missing"] is True


def test_clean_emits_nav_missing_records_for_share_and_seed_without_nav() -> None:
    records = EtfDailySizeCleaner().clean(
        [
            {
                "_source": SHARE_SOURCE,
                "_chunk_date": date(2024, 1, 2),
                "ts_code": "510300.SH",
                "trade_date": date(2024, 1, 2),
                "fd_share": "100",
            },
            {
                "_source": SEED_SOURCE,
                "_chunk_date": date(2024, 1, 2),
                "ts_code": "510500.SH",
                "trade_date": date(2023, 12, 30),
                "fd_share": "20",
            },
        ]
    )

    assert records == [
        {
            "trade_date": date(2024, 1, 2),
            "ts_code": "510300.SH",
            "fd_share": Decimal("100"),
            "share_source_date": date(2024, 1, 2),
            "nav_date": date(2024, 1, 2),
            "ann_date": None,
            "unit_nav": None,
            "aum_10k_cny": None,
            "aum_cny": None,
            "nav_missing": True,
        },
        {
            "trade_date": date(2024, 1, 2),
            "ts_code": "510500.SH",
            "fd_share": Decimal("20"),
            "share_source_date": date(2023, 12, 30),
            "nav_date": date(2024, 1, 2),
            "ann_date": None,
            "unit_nav": None,
            "aum_10k_cny": None,
            "aum_cny": None,
            "nav_missing": True,
        },
    ]


def test_clean_falls_back_to_seed_when_same_day_share_is_missing_or_non_finite() -> None:
    rows: list[dict[str, object]] = [
        _share_with_source("510300.SH", "20240102", None),
        _seed("510300.SH", "20231201", "111"),
        _share_with_source("510500.SH", "20240102", float("nan")),
        _seed("510500.SH", "20231202", "222"),
        _share_with_source("159915.SZ", "20240102", float("inf")),
        _seed("159915.SZ", "20231203", "333"),
        _share_with_source("512000.SH", "20240102", Decimal("Infinity")),
        _seed("512000.SH", "20231204", "444"),
    ]
    for ts_code in ("159915.SZ", "510300.SH", "510500.SH", "512000.SH"):
        rows.append(
            {
                "_source": NAV_SOURCE,
                "ts_code": ts_code,
                "nav_date": "20240102",
                "ann_date": "20240102",
                "unit_nav": "1.0",
            }
        )

    records = {record["ts_code"]: record for record in EtfDailySizeCleaner().clean(rows)}

    assert {code: record["fd_share"] for code, record in records.items()} == {
        "159915.SZ": Decimal("333"),
        "510300.SH": Decimal("111"),
        "510500.SH": Decimal("222"),
        "512000.SH": Decimal("444"),
    }
    assert records["159915.SZ"]["share_source_date"] == date(2023, 12, 3)
    assert records["510300.SH"]["share_source_date"] == date(2023, 12, 1)
    # A valid same-day share would still win over the seed.
    assert records["510500.SH"]["nav_missing"] is False


def test_clean_treats_nan_nat_ann_date_and_non_finite_unit_nav_as_missing() -> None:
    rows: list[dict[str, object]] = [
        _share_with_source("510300.SH", "20240102", "100"),
        _share_with_source("510500.SH", "20240102", "100"),
        _share_with_source("159915.SZ", "20240102", "100"),
        _share_with_source("512000.SH", "20240102", "100"),
        {
            "_source": NAV_SOURCE,
            "ts_code": "510300.SH",
            "nav_date": "20240102",
            "ann_date": float("nan"),
            "unit_nav": "1.5",
        },
        {
            "_source": NAV_SOURCE,
            "ts_code": "510500.SH",
            "nav_date": "20240102",
            "ann_date": _NaTLike(),
            "unit_nav": "2.0",
        },
        {
            "_source": NAV_SOURCE,
            "ts_code": "159915.SZ",
            "nav_date": "20240102",
            "ann_date": "20240102",
            "unit_nav": float("nan"),
        },
        {
            "_source": NAV_SOURCE,
            "ts_code": "512000.SH",
            "nav_date": "20240102",
            "ann_date": "20240102",
            "unit_nav": float("inf"),
        },
    ]

    records = {record["ts_code"]: record for record in EtfDailySizeCleaner().clean(rows)}

    # NaN/NaT-like ann_date is treated as an unpublished version.
    assert records["510300.SH"]["ann_date"] is None
    assert records["510300.SH"]["unit_nav"] == Decimal("1.5")
    assert records["510300.SH"]["nav_missing"] is False
    assert records["510500.SH"]["ann_date"] is None
    assert records["510500.SH"]["unit_nav"] == Decimal("2.0")
    assert records["510500.SH"]["nav_missing"] is False
    # Non-finite unit_nav is missing NAV, never an AUM.
    assert records["159915.SZ"]["unit_nav"] is None
    assert records["159915.SZ"]["aum_10k_cny"] is None
    assert records["159915.SZ"]["aum_cny"] is None
    assert records["159915.SZ"]["nav_missing"] is True
    assert records["512000.SH"]["unit_nav"] is None
    assert records["512000.SH"]["nav_missing"] is True


def test_clean_rejects_conflicting_same_version_unit_nav() -> None:
    cleaner = EtfDailySizeCleaner()
    share_row = _share_with_source("510300.SH", "20240102", "100")
    nav_template = {
        "_source": NAV_SOURCE,
        "ts_code": "510300.SH",
        "nav_date": "20240102",
    }

    with pytest.raises(ValueError, match="conflicting unit_nav"):
        cleaner.clean(
            [
                share_row,
                {**nav_template, "ann_date": "20240105", "unit_nav": "1.1"},
                {**nav_template, "ann_date": "20240105", "unit_nav": "1.2"},
            ]
        )

    with pytest.raises(ValueError, match="conflicting unit_nav"):
        cleaner.clean(
            [
                share_row,
                {**nav_template, "ann_date": "20240105", "unit_nav": None},
                {**nav_template, "ann_date": "20240105", "unit_nav": "1.2"},
            ]
        )


def test_clean_rejects_conflicting_duplicate_shares_and_mismatched_dates() -> None:
    cleaner = EtfDailySizeCleaner()
    nav_row = {
        "_source": NAV_SOURCE,
        "ts_code": "510300.SH",
        "nav_date": "20240102",
        "ann_date": "20240102",
        "unit_nav": "1.0",
    }

    with pytest.raises(ValueError, match="conflicting fd_share"):
        cleaner.clean(
            [
                _share_with_source("510300.SH", "20240102", "100"),
                _share_with_source("510300.SH", "20240102", "200"),
                nav_row,
            ]
        )

    # Identical duplicates collapse instead of conflicting.
    collapsed = cleaner.clean(
        [
            _share_with_source("510300.SH", "20240102", "100"),
            _share_with_source("510300.SH", "20240102", "100"),
            nav_row,
        ]
    )
    assert len(collapsed) == 1

    with pytest.raises(ValueError, match="does not match the chunk trade date"):
        cleaner.clean([_share_with_source("510300.SH", "20240103", "100"), nav_row])

    # NAV-only rows without any share never widen the universe, even when their
    # nav dates disagree with each other (they are not ETF candidates at all).
    assert (
        cleaner.clean(
            [
                {**nav_row, "ts_code": "510300.SH"},
                {**nav_row, "ts_code": "510500.SH", "nav_date": "20240103"},
            ]
        )
        == []
    )


def test_clean_rejects_seed_after_chunk_date_and_unknown_sources() -> None:
    cleaner = EtfDailySizeCleaner()
    nav_row = {
        "_source": NAV_SOURCE,
        "ts_code": "510300.SH",
        "nav_date": "20240102",
        "ann_date": "20240102",
        "unit_nav": "1.0",
    }

    with pytest.raises(ValueError, match="dated after the chunk trade date"):
        cleaner.clean([_seed("510300.SH", "20240103", "100"), nav_row])

    with pytest.raises(ValueError, match="unsupported record source"):
        cleaner.clean([{"_source": "bogus"}])


# --- Pipeline: chunking, DB seed window, universe union, backward backfill -------


def test_plan_chunks_one_trade_day_per_chunk() -> None:
    pipeline = EtfDailySizePipeline(
        calendar=_calendar({date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)}),
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
        engine=_sqlite_engine(),
        target_table=etf_daily_size,
    )

    chunks = pipeline.plan_chunks({"params": {"start_date": "20240102", "end_date": "20240104"}})

    assert chunks == [
        {"params": {"trade_date": "2024-01-02"}},
        {"params": {"trade_date": "2024-01-03"}},
        {"params": {"trade_date": "2024-01-04"}},
    ]


def test_fetch_uses_same_day_shares_without_db_or_backfill() -> None:
    client = FakeTushareClient(
        share_rows={
            ("20240102", "SH"): [_share("510300.SH", "20240102", 10)],
            ("20240102", "SZ"): [_share("159915.SZ", "20240102", 20)],
        },
        nav_rows={
            "20240102": [
                _nav("510300.SH", "20240102", 1.0, "20240102"),
                _nav("159915.SZ", "20240102", 2.0, "20240102"),
            ]
        },
    )
    pipeline = _PipelineWithoutDbSeeds(
        calendar=_calendar({date(2024, 1, 2)}),
        client=client,
        retry_policy=RetryPolicy(),
        engine=_sqlite_engine(),
        target_table=etf_daily_size,
    )

    rows = pipeline.fetch({"params": {"trade_date": "2024-01-02"}})

    assert {row["_source"] for row in rows} == {SHARE_SOURCE, NAV_SOURCE}
    assert len(rows) == 4
    assert {row["_chunk_date"] for row in rows} == {date(2024, 1, 2)}
    assert client.share_calls == [
        ("20240102", "SH", _SHARE_FIELDS),
        ("20240102", "SZ", _SHARE_FIELDS),
    ]


def test_fetch_ignores_nav_only_codes_without_share_or_backfill() -> None:
    """fund_nav market E also returns non-ETF funds: NAV-only codes are not ETFs.

    A NAV-only code must not trigger historical fund_share lookups, must not
    produce a record, and must not be mistaken for a known ETF.
    """
    client = FakeTushareClient(
        share_rows={("20240104", "SH"): [_share("510300.SH", "20240104", "10")]},
        nav_rows={
            "20240104": [
                _nav("510300.SH", "20240104", 1.5, "20240104"),
                # e.g. a closed-end fund that is not an ETF and never appears in fund_share.
                _nav("500001.SH", "20240104", 2.0, "20240104"),
            ]
        },
    )
    pipeline = _PipelineWithoutDbSeeds(
        calendar=_calendar({date(2024, 1, 3), date(2024, 1, 4)}),
        client=client,
        retry_policy=RetryPolicy(),
        engine=_sqlite_engine(),
        target_table=etf_daily_size,
    )

    rows = pipeline.fetch({"params": {"trade_date": "2024-01-04"}})

    # No seed rows at all: the NAV-only code triggers no backward scan.
    assert [row for row in rows if row["_source"] == SEED_SOURCE] == []
    assert [call[0] for call in client.share_calls] == ["20240104", "20240104"]

    cleaned = pipeline.clean(rows)
    assert [record["ts_code"] for record in cleaned] == ["510300.SH"]
    assert cleaned[0]["fd_share"] == Decimal("10")


def test_fetch_cleaner_ignores_nav_only_codes_without_any_share_rows() -> None:
    """Cleaner-level regression: NAV-only codes never widen the output universe."""
    cleaned = EtfDailySizeCleaner().clean(
        [
            {
                "_source": SHARE_SOURCE,
                "_chunk_date": date(2024, 1, 4),
                "ts_code": "510300.SH",
                "trade_date": date(2024, 1, 4),
                "fd_share": "10",
            },
            {
                "_source": NAV_SOURCE,
                "_chunk_date": date(2024, 1, 4),
                "ts_code": "510300.SH",
                "nav_date": date(2024, 1, 4),
                "ann_date": date(2024, 1, 4),
                "unit_nav": "1.5",
            },
            {
                "_source": NAV_SOURCE,
                "_chunk_date": date(2024, 1, 4),
                "ts_code": "500001.SH",
                "nav_date": date(2024, 1, 4),
                "ann_date": date(2024, 1, 4),
                "unit_nav": "2.0",
            },
        ]
    )

    assert [record["ts_code"] for record in cleaned] == ["510300.SH"]


def test_fetch_skips_non_finite_share_without_db_seed() -> None:
    """A non-finite same-day share with no stored seed is skipped, not backfilled."""
    client = FakeTushareClient(
        share_rows={
            ("20240104", "SH"): [_share("510500.SH", "20240104", float("nan"))],
        },
        nav_rows={"20240104": [_nav("510500.SH", "20240104", 1.5, "20240104")]},
    )
    pipeline = _PipelineWithoutDbSeeds(
        calendar=_calendar({date(2024, 1, 3), date(2024, 1, 4)}),
        client=client,
        retry_policy=RetryPolicy(),
        engine=_sqlite_engine(),
        target_table=etf_daily_size,
    )

    rows = pipeline.fetch({"params": {"trade_date": "2024-01-04"}})

    assert [row for row in rows if row["_source"] == SEED_SOURCE] == []
    assert pipeline.clean(rows) == []
    assert [call[0] for call in client.share_calls] == ["20240104", "20240104"]


def test_fetch_universe_left_joins_nav_and_keeps_seed_only_codes(
    postgres_engine: Engine,
) -> None:
    """Current shares + DB seeds form the universe; NAV-only codes stay out."""
    with postgres_engine.begin() as connection:
        connection.execute(
            etf_daily_size.insert(),
            [
                {
                    "trade_date": date(2024, 1, 1),
                    "ts_code": "510500.SH",
                    "fd_share": Decimal("50"),
                    "share_source_date": date(2023, 12, 30),
                },
            ],
        )

    client = FakeTushareClient(
        share_rows={
            ("20240104", "SH"): [_share("510300.SH", "20240104", "10")],
            # Available history for the NAV-only non-ETF code 500001.SH: it must
            # still never be used, because fund_share alone defines the universe.
            ("20240103", "SH"): [_share("500001.SH", "20240103", "7")],
        },
        nav_rows={
            "20240104": [
                _nav("510300.SH", "20240104", 1.0, "20240104"),
                _nav("500001.SH", "20240104", 3.0, "20240104"),
            ]
        },
    )
    pipeline = EtfDailySizePipeline(
        calendar=_calendar({date(2024, 1, 3), date(2024, 1, 4)}),
        client=client,
        retry_policy=RetryPolicy(),
        engine=postgres_engine,
        target_table=etf_daily_size,
    )

    rows = pipeline.fetch({"params": {"trade_date": "2024-01-04"}})

    assert [row for row in rows if row["_source"] == SEED_SOURCE] == [
        _seed_row("510500.SH", date(2023, 12, 30), Decimal("50")),
    ]
    # Only the chunk-day fetch: no backward scan for the NAV-only code.
    assert client.share_calls == [
        ("20240104", "SH", _SHARE_FIELDS),
        ("20240104", "SZ", _SHARE_FIELDS),
    ]

    cleaned = {record["ts_code"]: record for record in pipeline.clean(rows)}
    assert set(cleaned) == {"510300.SH", "510500.SH"}
    assert cleaned["510300.SH"]["nav_missing"] is False
    # DB seed survives the left join even without any NAV row on D.
    assert cleaned["510500.SH"] == {
        "trade_date": date(2024, 1, 4),
        "ts_code": "510500.SH",
        "fd_share": Decimal("50"),
        "share_source_date": date(2023, 12, 30),
        "nav_date": date(2024, 1, 4),
        "ann_date": None,
        "unit_nav": None,
        "aum_10k_cny": None,
        "aum_cny": None,
        "nav_missing": True,
    }


def test_fetch_treats_non_finite_same_day_share_as_missing(
    postgres_engine: Engine,
) -> None:
    with postgres_engine.begin() as connection:
        connection.execute(
            etf_daily_size.insert(),
            [
                {
                    "trade_date": date(2024, 1, 1),
                    "ts_code": "510300.SH",
                    "fd_share": Decimal("100"),
                    "share_source_date": None,
                },
                {
                    "trade_date": date(2024, 1, 1),
                    "ts_code": "159915.SZ",
                    "fd_share": Decimal("30"),
                    "share_source_date": None,
                },
            ],
        )

    client = FakeTushareClient(
        share_rows={
            ("20240104", "SH"): [
                _share("510300.SH", "20240104", float("nan")),
                _share("510500.SH", "20240104", float("inf")),
                _share("512000.SH", "20240104", Decimal("Infinity")),
            ],
        },
        nav_rows={
            "20240104": [
                _nav("510300.SH", "20240104", 1.0, "20240104"),
                _nav("510500.SH", "20240104", 1.0, "20240104"),
                _nav("512000.SH", "20240104", 1.0, "20240104"),
            ]
        },
    )
    pipeline = EtfDailySizePipeline(
        calendar=_calendar({date(2024, 1, 3), date(2024, 1, 4)}),
        client=client,
        retry_policy=RetryPolicy(),
        engine=postgres_engine,
        target_table=etf_daily_size,
    )

    rows = pipeline.fetch({"params": {"trade_date": "2024-01-04"}})

    seeds = {row["ts_code"]: row for row in rows if row["_source"] == SEED_SOURCE}
    assert seeds["510300.SH"]["trade_date"] == date(2024, 1, 1)
    assert seeds["510300.SH"]["fd_share"] == Decimal("100")
    assert seeds["159915.SZ"]["fd_share"] == Decimal("30")
    # Non-finite same-day rows never become seeds or trigger historical scans.
    assert "510500.SH" not in seeds
    assert "512000.SH" not in seeds

    cleaned = {record["ts_code"]: record for record in pipeline.clean(rows)}
    # NaN share never masks the DB seed.
    assert cleaned["510300.SH"]["fd_share"] == Decimal("100")
    assert cleaned["510300.SH"]["share_source_date"] == date(2024, 1, 1)
    assert cleaned["159915.SZ"]["nav_missing"] is True
    # No share anywhere: skipped, never zero-filled.
    assert "512000.SH" not in cleaned
    assert "510500.SH" not in cleaned


def test_fetch_loads_latest_db_seed_before_chunk_date(postgres_engine: Engine) -> None:
    with postgres_engine.begin() as connection:
        connection.execute(
            etf_daily_size.insert(),
            [
                {
                    "trade_date": date(2024, 1, 1),
                    "ts_code": "510500.SH",
                    "fd_share": Decimal("50"),
                    "share_source_date": date(2023, 12, 30),
                },
                {
                    "trade_date": date(2024, 1, 2),
                    "ts_code": "510300.SH",
                    "fd_share": Decimal("100"),
                    "share_source_date": None,
                },
                {
                    "trade_date": date(2024, 1, 3),
                    "ts_code": "510300.SH",
                    "fd_share": Decimal("200"),
                    "share_source_date": None,
                },
                # Future rows must never seed an earlier chunk date.
                {
                    "trade_date": date(2024, 1, 5),
                    "ts_code": "510300.SH",
                    "fd_share": Decimal("999"),
                    "share_source_date": None,
                },
                {
                    "trade_date": date(2024, 1, 5),
                    "ts_code": "159915.SZ",
                    "fd_share": Decimal("777"),
                    "share_source_date": None,
                },
            ],
        )

    client = FakeTushareClient(
        nav_rows={
            "20240104": [
                _nav("510300.SH", "20240104", 1.5, "20240104"),
                _nav("510500.SH", "20240104", 2.0, "20240104"),
                _nav("159915.SZ", "20240104", 3.0, "20240104"),
            ]
        },
    )
    pipeline = EtfDailySizePipeline(
        calendar=_calendar({date(2024, 1, 4)}),
        client=client,
        retry_policy=RetryPolicy(),
        engine=postgres_engine,
        target_table=etf_daily_size,
    )

    rows = pipeline.fetch({"params": {"trade_date": "2024-01-04"}})

    seeds = {row["ts_code"]: row for row in rows if row["_source"] == SEED_SOURCE}
    # The window query keeps the latest trade_date < D per code.
    assert seeds["510300.SH"]["trade_date"] == date(2024, 1, 3)
    assert seeds["510300.SH"]["fd_share"] == Decimal("200")
    # share_source_date (not the row trade_date) is carried forward.
    assert seeds["510500.SH"]["trade_date"] == date(2023, 12, 30)
    assert seeds["510500.SH"]["fd_share"] == Decimal("50")
    # Only future rows exist for 159915.SZ, so it gets no seed.
    assert "159915.SZ" not in seeds

    cleaned = pipeline.clean(rows)
    assert {record["ts_code"] for record in cleaned} == {"510300.SH", "510500.SH"}
    assert cleaned[0]["share_source_date"] == date(2024, 1, 3)
    assert cleaned[0]["nav_date"] == date(2024, 1, 4)
    assert client.share_calls == [
        ("20240104", "SH", _SHARE_FIELDS),
        ("20240104", "SZ", _SHARE_FIELDS),
    ]


# --- Spec / mapping / app wiring -------------------------------------------------


def test_task_spec_and_pipeline_mapping_expose_etf_daily_size() -> None:
    mapping_path = Path(__file__).resolve().parents[1] / "config" / "task_pipeline_mapping.py"

    mapping = load_pipeline_mapping(str(mapping_path))

    assert TaskSpec.GET_ETF_DAILY_SIZE == "get_etf_daily_size"
    assert mapping[TaskSpec.GET_ETF_DAILY_SIZE] == ["etf_daily_size"]


def test_create_app_registers_etf_daily_size_pipeline(
    monkeypatch: pytest.MonkeyPatch, postgres_engine: Engine
) -> None:
    calendar = _calendar({date(2024, 1, 2), date(2024, 1, 3)})
    client = FakeTushareClient()

    monkeypatch.setattr(api_main, "create_engine_from_config", lambda config: postgres_engine)
    monkeypatch.setattr(
        api_main,
        "build_calendar_service",
        lambda config: SimpleNamespace(calendar=calendar),
    )
    monkeypatch.setattr(api_main, "TushareProClient", lambda token: client)

    with TestClient(api_main.create_app()) as app_client:
        registry = app_client.app.state.pipeline_registry
        pipeline = registry.get("etf_daily_size")

        assert isinstance(pipeline, EtfDailySizePipeline)
        assert pipeline.engine is postgres_engine
        assert pipeline.target_table is etf_daily_size
        assert app_client.app.state.pipeline_selector.candidates_for(
            TaskSpec.GET_ETF_DAILY_SIZE
        ) == ["etf_daily_size"]
