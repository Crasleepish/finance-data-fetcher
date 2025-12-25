from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Sequence

import pytest

from core.calendar.service import TradingCalendarService


@dataclass
class FakeCalendarStore:
    dates: set[date] = field(default_factory=set)

    def get_bounds(self) -> tuple[date, date] | None:
        if not self.dates:
            return None
        return min(self.dates), max(self.dates)

    def get_trade_days(self, start: date, end: date) -> list[date]:
        return sorted(day for day in self.dates if start <= day <= end)

    def is_trade_day(self, day: date) -> bool:
        return day in self.dates

    def prev_trade_day(self, day: date) -> date | None:
        candidates = [value for value in self.dates if value < day]
        return max(candidates) if candidates else None

    def next_trade_day(self, day: date) -> date | None:
        candidates = [value for value in self.dates if value > day]
        return min(candidates) if candidates else None

    def insert_trade_days(self, days: Sequence[date]) -> int:
        before = len(self.dates)
        self.dates.update(days)
        return len(self.dates) - before


@dataclass
class FakeSyncer:
    response: list[date]
    calls: list[tuple[date, date, str]] = field(default_factory=list)

    def fetch_trade_days(self, start: date, end: date, exchange: str) -> list[date]:
        self.calls.append((start, end, exchange))
        return list(self.response)


def test_is_trade_day_triggers_sync() -> None:
    store = FakeCalendarStore()
    syncer = FakeSyncer(response=[date(2024, 1, 2)])
    service = TradingCalendarService(store=store, syncer=syncer)

    assert service.is_trade_day(date(2024, 1, 2)) is True
    assert syncer.calls == [(date(2024, 1, 2), date(2024, 1, 2), "SSE")]


def test_prev_next_trade_day() -> None:
    store = FakeCalendarStore({date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5)})
    syncer = FakeSyncer(response=[])
    service = TradingCalendarService(store=store, syncer=syncer)

    assert service.prev_trade_day(date(2024, 1, 5)) == date(2024, 1, 3)
    assert service.next_trade_day(date(2024, 1, 3)) == date(2024, 1, 5)


def test_normalize_trade_day_chunks() -> None:
    store = FakeCalendarStore()
    syncer = FakeSyncer(response=[date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5)])
    service = TradingCalendarService(store=store, syncer=syncer)

    chunks = service.normalize_trade_day_chunks(date(2024, 1, 1), date(2024, 1, 6), 2)

    assert chunks == [[date(2024, 1, 2), date(2024, 1, 3)], [date(2024, 1, 5)]]


def test_normalize_trade_day_chunks_invalid_range() -> None:
    store = FakeCalendarStore()
    syncer = FakeSyncer(response=[])
    service = TradingCalendarService(store=store, syncer=syncer)

    with pytest.raises(ValueError, match="start must be on or before end"):
        service.normalize_trade_day_chunks(date(2024, 1, 2), date(2024, 1, 1))
