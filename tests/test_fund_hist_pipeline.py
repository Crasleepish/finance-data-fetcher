from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from unittest.mock import Mock

from core.clean.fund_hist_cleaner import FundHistCleaner
from core.fetch.retry import RetryPolicy
from infra.db.tables import fund_info
from infra.fetcher.tushare_fund_nav_fetcher import TushareFundNavFetcher
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


def test_fund_nav_fetcher_batches_codes() -> None:
    client = FakeTushareClient()
    fetcher = TushareFundNavFetcher(client=client, retry_policy=RetryPolicy(), batch_size=2)
    rows = fetcher.fetch({"params": {"nav_date": "2024-01-02", "codes": ["A", "B", "C"]}})

    assert [row["ts_code"] for row in rows] == ["A", "B", "C"]
    assert client.calls == [("A,B", "20240102"), ("C", "20240102")]


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
