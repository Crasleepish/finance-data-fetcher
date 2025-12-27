from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

from core.calendar.service import TradingCalendarService
from core.pipeline.types import Arguments, ChunkArgs


class ChunkPolicy(Protocol):
    """Policy interface for planning chunk arguments."""

    def plan(self, arguments: Arguments) -> list[ChunkArgs]:
        """Return chunk arguments for a given request."""


@dataclass(frozen=True)
class RangeChunkPolicy:
    """Split a numeric or date range into fixed-size chunks."""

    start_key: str
    end_key: str
    step: int
    unit: str = "number"

    def plan(self, arguments: Arguments) -> list[ChunkArgs]:
        params = _get_params(arguments)
        start_value = _require_key(params, self.start_key)
        end_value = _require_key(params, self.end_key)

        if self.unit == "day":
            start = _parse_date(start_value)
            end = _parse_date(end_value)
            return _chunk_dates(start, end, self.step, self.start_key, self.end_key)

        start_num = _parse_int(start_value)
        end_num = _parse_int(end_value)
        return _chunk_numbers(start_num, end_num, self.step, self.start_key, self.end_key)


@dataclass(frozen=True)
class PaginationChunkPolicy:
    """Split a paginated request into offset/limit chunks."""

    offset_key: str = "offset"
    limit_key: str = "limit"
    total_key: str = "total_count"

    def plan(self, arguments: Arguments) -> list[ChunkArgs]:
        params = _get_params(arguments)
        total = _parse_int(_require_key(params, self.total_key))
        limit = _parse_int(_require_key(params, self.limit_key))
        if limit <= 0:
            raise ValueError("limit must be positive")
        chunks: list[ChunkArgs] = []
        for offset in range(0, total, limit):
            chunk_params = dict(params)
            chunk_params[self.offset_key] = offset
            chunk_params[self.limit_key] = limit
            chunks.append({"params": chunk_params})
        return chunks


@dataclass(frozen=True)
class TradeDayRangeChunkPolicy:
    """Split a date range into trade-day-aware chunks."""

    start_key: str
    end_key: str
    chunk_size: int
    calendar: TradingCalendarService

    def plan(self, arguments: Arguments) -> list[ChunkArgs]:
        params = _get_params(arguments)
        start = _parse_date(_require_key(params, self.start_key))
        end = _parse_date(_require_key(params, self.end_key))
        chunks = self.calendar.normalize_trade_day_chunks(start, end, self.chunk_size)
        return [
            {
                "params": {
                    **params,
                    self.start_key: chunk[0].isoformat(),
                    self.end_key: chunk[-1].isoformat(),
                }
            }
            for chunk in chunks
            if chunk
        ]


def _get_params(arguments: Arguments) -> dict[str, object]:
    return dict(arguments.get("params", {}))


def _require_key(params: dict[str, object], key: str) -> object:
    if key not in params:
        raise KeyError(f"missing required param: {key}")
    return params[key]


def _parse_int(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("expected int, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError("expected integer-like value")


def _parse_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("expected date or YYYY-MM-DD string")


def _chunk_numbers(
    start: int,
    end: int,
    step: int,
    start_key: str,
    end_key: str,
) -> list[ChunkArgs]:
    if step <= 0:
        raise ValueError("step must be positive")
    if start > end:
        raise ValueError("start must be <= end")
    chunks: list[ChunkArgs] = []
    current = start
    while current <= end:
        chunk_end = min(current + step - 1, end)
        chunks.append({"params": {start_key: current, end_key: chunk_end}})
        current = chunk_end + 1
    return chunks


def _chunk_dates(
    start: date,
    end: date,
    step: int,
    start_key: str,
    end_key: str,
) -> list[ChunkArgs]:
    if step <= 0:
        raise ValueError("step must be positive")
    if start > end:
        raise ValueError("start must be <= end")
    chunks: list[ChunkArgs] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=step - 1), end)
        chunks.append(
            {
                "params": {
                    start_key: current.isoformat(),
                    end_key: chunk_end.isoformat(),
                }
            }
        )
        current = chunk_end + timedelta(days=1)
    return chunks
