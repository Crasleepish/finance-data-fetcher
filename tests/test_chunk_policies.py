from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pytest

from core.calendar.service import TradingCalendarService
from core.chunking.policies import PaginationChunkPolicy, RangeChunkPolicy, TradeDayRangeChunkPolicy
from core.pipeline.types import Arguments


@dataclass(frozen=True)
class DummyCalendarStore:
    def get_bounds(self) -> tuple[date, date] | None:
        return None

    def get_trade_days(self, start: date, end: date) -> list[date]:
        return [start, end]

    def is_trade_day(self, day: date) -> bool:
        return True

    def prev_trade_day(self, day: date) -> date | None:
        return day

    def next_trade_day(self, day: date) -> date | None:
        return day

    def insert_trade_days(self, days: list[date]) -> int:
        return len(days)


@dataclass(frozen=True)
class DummyCalendarSyncer:
    def fetch_trade_days(self, start: date, end: date, exchange: str) -> list[date]:
        return [start, end]


def test_range_chunk_policy_numbers() -> None:
    policy = RangeChunkPolicy(start_key="start", end_key="end", step=3)
    arguments: Arguments = {"params": {"start": 1, "end": 7}}
    chunks = policy.plan(arguments)
    assert chunks == [
        {"params": {"start": 1, "end": 3}},
        {"params": {"start": 4, "end": 6}},
        {"params": {"start": 7, "end": 7}},
    ]


def test_range_chunk_policy_dates() -> None:
    policy = RangeChunkPolicy(start_key="start_date", end_key="end_date", step=2, unit="day")
    arguments: Arguments = {"params": {"start_date": "2024-01-01", "end_date": "2024-01-03"}}
    chunks = policy.plan(arguments)
    assert chunks == [
        {"params": {"start_date": "2024-01-01", "end_date": "2024-01-02"}},
        {"params": {"start_date": "2024-01-03", "end_date": "2024-01-03"}},
    ]


def test_pagination_chunk_policy() -> None:
    policy = PaginationChunkPolicy()
    arguments: Arguments = {"params": {"offset": 0, "limit": 2, "total_count": 5}}
    chunks = policy.plan(arguments)
    assert chunks == [
        {"params": {"offset": 0, "limit": 2, "total_count": 5}},
        {"params": {"offset": 2, "limit": 2, "total_count": 5}},
        {"params": {"offset": 4, "limit": 2, "total_count": 5}},
    ]


def test_trade_day_chunk_policy() -> None:
    calendar = TradingCalendarService(store=DummyCalendarStore(), syncer=DummyCalendarSyncer())
    policy = TradeDayRangeChunkPolicy(
        start_key="start_date",
        end_key="end_date",
        chunk_size=10,
        calendar=calendar,
    )
    arguments: Arguments = {"params": {"start_date": "2024-01-02", "end_date": "2024-01-05"}}
    chunks = policy.plan(arguments)
    assert chunks == [
        {"params": {"start_date": "2024-01-02", "end_date": "2024-01-05"}},
    ]


def test_range_chunk_policy_missing_key() -> None:
    policy = RangeChunkPolicy(start_key="start", end_key="end", step=2)
    arguments: Arguments = {"params": {"start": 1}}
    with pytest.raises(KeyError):
        policy.plan(arguments)
