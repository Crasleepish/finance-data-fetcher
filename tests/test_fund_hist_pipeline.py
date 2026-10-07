from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from unittest.mock import Mock

from core.clean.fund_hist_cleaner import FundHistCleaner
from core.fetch.retry import RetryPolicy
from infra.db.tables import fund_info
from infra.fetcher.tushare_fund_nav_fetcher import (
    TushareFundNavFetcher,
    TushareFundNavHistoryFetcher,
    _filter_within_end,
)
from services.pipelines.fund_hist_index_pipeline import FundHistIndexPipeline


@dataclass
class FakeTushareClient:
    calls: list[tuple[str, str]] = field(default_factory=list)

    def fund_nav(self, ts_code: str, nav_date: str, fields: str) -> list[dict[str, object]]:
        self.calls.append((ts_code, nav_date))
        rows = []
        for code in ts_code.split(","):
            rows.append({"ts_code": code, "nav_date": nav_date, "unit_nav": 1.0, "adj_nav": 1.2})
        return rows


@dataclass
class FakeHistoryClient:
    rows: list[dict[str, object]] = field(default_factory=list)
    calls: list[tuple[str, str, str, str]] = field(default_factory=list)

    def fund_nav_history(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        self.calls.append((ts_code, start_date, end_date, fields))
        return self.rows


def test_fund_nav_fetcher_batches_codes() -> None:
    client = FakeTushareClient()
    fetcher = TushareFundNavFetcher(client=client, retry_policy=RetryPolicy(), batch_size=2)
    rows = fetcher.fetch({"params": {"nav_date": "2024-01-02", "codes": ["A", "B", "C"]}})

    assert [row["ts_code"] for row in rows] == ["A", "B", "C"]
    assert client.calls == [("A,B", "20240102"), ("C", "20240102")]


def test_history_fetcher_passes_window_to_client() -> None:
    rows_in_window = [
        {"ts_code": "161207.OF", "nav_date": "20090105", "unit_nav": 1.0, "adj_nav": 1.1},
        {"ts_code": "161207.OF", "nav_date": "20251231", "unit_nav": 2.0, "adj_nav": 2.1},
    ]
    client = FakeHistoryClient(rows=rows_in_window)
    fetcher = TushareFundNavHistoryFetcher(client=client, retry_policy=RetryPolicy())

    result = fetcher.fetch(
        {
            "params": {
                "ts_code": "161207.OF",
                "start_date": "2009-01-01",
                "end_date": "2025-12-31",
            }
        }
    )

    assert client.calls == [
        ("161207.OF", "20090101", "20251231", "ts_code,nav_date,unit_nav,adj_nav")
    ]
    assert result == rows_in_window


def test_history_fetcher_drops_rows_after_end_date() -> None:
    client = FakeHistoryClient(
        rows=[
            {"ts_code": "161207.OF", "nav_date": "20251231", "unit_nav": 1.0, "adj_nav": 1.1},
            {"ts_code": "161207.OF", "nav_date": "20260101", "unit_nav": 2.0, "adj_nav": 2.1},
        ]
    )
    fetcher = TushareFundNavHistoryFetcher(client=client, retry_policy=RetryPolicy())

    result = fetcher.fetch(
        {
            "params": {
                "ts_code": "161207.OF",
                "start_date": "2009-01-01",
                "end_date": "2025-12-31",
            }
        }
    )

    assert [row["nav_date"] for row in result] == ["20251231"]


def test_history_fetcher_drops_unparseable_nav_date() -> None:
    client = FakeHistoryClient(
        rows=[
            {"ts_code": "161207.OF", "nav_date": "20240102", "unit_nav": 1.0, "adj_nav": 1.1},
            {"ts_code": "161207.OF", "unit_nav": 1.0, "adj_nav": 1.1},
            {"ts_code": "161207.OF", "nav_date": "", "unit_nav": 1.0, "adj_nav": 1.1},
            {"ts_code": "161207.OF", "nav_date": "not-a-date", "unit_nav": 1.0, "adj_nav": 1.1},
        ]
    )
    fetcher = TushareFundNavHistoryFetcher(client=client, retry_policy=RetryPolicy())

    result = fetcher.fetch(
        {
            "params": {
                "ts_code": "161207.OF",
                "start_date": "2009-01-01",
                "end_date": "2025-12-31",
            }
        }
    )

    assert [row["nav_date"] for row in result] == ["20240102"]


def test_filter_within_end_is_pure() -> None:
    rows = [
        {"nav_date": "20251231"},
        {"nav_date": "20260101"},
        {"nav_date": "bad"},
        {},
    ]
    assert _filter_within_end(rows, "20251231") == [{"nav_date": "20251231"}]


def test_fund_hist_cleaner_maps_fields() -> None:
    cleaner = FundHistCleaner()
    raw = [
        {"ts_code": "000001.OF", "nav_date": "20240102", "unit_nav": 1.1, "adj_nav": 1.2},
        {"ts_code": "000001.OF", "nav_date": "20240102", "unit_nav": 1.3, "adj_nav": 1.4},
    ]
    cleaned = list(cleaner.clean(raw))
    assert cleaned == [
        {"fund_code": "000001.OF", "date": date(2024, 1, 2), "value": 1.3, "net_value": 1.4}
    ]


def test_fund_hist_index_requires_fund_info_codes(postgres_engine) -> None:
    calendar = Mock()
    calendar.normalize_trade_day_chunks.return_value = []
    pipeline = FundHistIndexPipeline(
        calendar=calendar,
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
        engine=postgres_engine,
        fund_info_table=fund_info,
    )

    try:
        pipeline.plan_chunks({"params": {"start_date": "2024-01-02", "end_date": "2024-01-03"}})
    except ValueError as exc:
        assert "fund_hist_index requires fund_info" in str(exc)
    else:
        raise AssertionError("expected ValueError when fund_info has no index fund codes")
