from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol, Sequence


class CalendarStore(Protocol):
    def get_bounds(self) -> tuple[date, date] | None: ...

    def get_trade_days(self, start: date, end: date) -> list[date]: ...

    def is_trade_day(self, day: date) -> bool: ...

    def prev_trade_day(self, day: date) -> date | None: ...

    def next_trade_day(self, day: date) -> date | None: ...

    def insert_trade_days(self, days: Sequence[date]) -> int: ...


class CalendarSyncer(Protocol):
    def fetch_trade_days(self, start: date, end: date, exchange: str) -> list[date]: ...


@dataclass(frozen=True)
class TradingCalendarService:
    store: CalendarStore
    syncer: CalendarSyncer
    exchange: str = "SSE"

    def is_trade_day(self, day: date) -> bool:
        self._ensure_range(day, day)
        return self.store.is_trade_day(day)

    def prev_trade_day(self, day: date) -> date | None:
        self._ensure_range(day, day)
        return self.store.prev_trade_day(day)

    def next_trade_day(self, day: date) -> date | None:
        self._ensure_range(day, day)
        return self.store.next_trade_day(day)

    def normalize_trade_day_chunks(
        self, start: date, end: date, chunk_size: int = 100
    ) -> list[list[date]]:
        if start > end:
            raise ValueError("start must be on or before end")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        self._ensure_range(start, end)
        trade_days = self.store.get_trade_days(start, end)
        return _chunk_dates(trade_days, chunk_size)

    def sync_range(self, start: date, end: date, exchange: str | None = None) -> int:
        if start > end:
            raise ValueError("start must be on or before end")
        resolved_exchange = exchange or self.exchange
        trade_days = self.syncer.fetch_trade_days(start, end, resolved_exchange)
        return self.store.insert_trade_days(trade_days)

    def _ensure_range(self, start: date, end: date) -> None:
        bounds = self.store.get_bounds()
        if bounds is None:
            self.sync_range(start, end)
            return

        min_date, max_date = bounds
        if start < min_date:
            self.sync_range(start, min_date - timedelta(days=1))
        if end > max_date:
            self.sync_range(max_date + timedelta(days=1), end)


def _chunk_dates(dates: Sequence[date], chunk_size: int) -> list[list[date]]:
    chunks: list[list[date]] = []
    for i in range(0, len(dates), chunk_size):
        chunks.append(list(dates[i : i + chunk_size]))
    return chunks
