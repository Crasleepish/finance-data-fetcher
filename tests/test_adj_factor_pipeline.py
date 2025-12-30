from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from core.calendar.service import TradingCalendarService
from core.clean.adj_factor_cleaner import AdjFactorCleaner
from core.fetch.retry import RetryPolicy
from infra.fetcher.tushare_adj_factor_fetcher import TushareAdjFactorFetcher
from services.pipelines.adj_factor_pipeline import AdjFactorPipeline


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
    calls: list[tuple[str, str]] = field(default_factory=list)

    def adj_factor(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        self.calls.append((trade_date, fields))
        if trade_date == "20240102":
            return [{"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 123.45}]
        return []


def test_adj_factor_fetcher_uses_trade_date() -> None:
    client = FakeTushareClient()
    fetcher = TushareAdjFactorFetcher(client=client, retry_policy=RetryPolicy())
    rows = fetcher.fetch({"params": {"trade_date": "2024-01-02"}})

    assert rows == [{"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 123.45}]
    assert client.calls == [("20240102", "ts_code,trade_date,adj_factor")]


def test_adj_factor_cleaner_maps_fields() -> None:
    cleaner = AdjFactorCleaner()
    raw = [{"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 123.45}]
    cleaned = list(cleaner.clean(raw))

    assert cleaned == [{"stock_code": "000001.SZ", "date": date(2024, 1, 2), "adj_factor": 123.45}]


def test_adj_factor_pipeline_plans_trade_day_chunks() -> None:
    store = FakeCalendarStore(dates={date(2024, 1, 2), date(2024, 1, 3)})
    calendar = TradingCalendarService(store=store, syncer=FakeSyncer())
    pipeline = AdjFactorPipeline(
        calendar=calendar,
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
    )

    chunks = pipeline.plan_chunks(
        {"params": {"start_date": "2024-01-02", "end_date": "2024-01-03"}}
    )

    assert chunks == [
        {"params": {"trade_date": "2024-01-02"}},
        {"params": {"trade_date": "2024-01-03"}},
    ]
