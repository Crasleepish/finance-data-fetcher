from __future__ import annotations

from datetime import datetime
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class RtIndexHistCleaner:
    """Clean and normalize real-time index snapshot rows."""

    def __init__(self) -> None:
        optional_float = (float, int, type(None))
        optional_int = (int, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "index_code": "index_code",
                "open": "open",
                "close": "close",
                "high": "high",
                "low": "low",
                "pre_close": "pre_close",
                "volume": "volume",
                "amount": "amount",
                "latest_time": "latest_time",
            },
            type_map={
                "index_code": str,
                "open": optional_float,
                "close": optional_float,
                "high": optional_float,
                "low": optional_float,
                "pre_close": optional_float,
                "volume": optional_int,
                "amount": optional_float,
                "latest_time": datetime,
            },
            required_fields={"index_code", "latest_time"},
            casts={
                "open": _to_float,
                "close": _to_float,
                "high": _to_float,
                "low": _to_float,
                "pre_close": _to_float,
                "volume": _to_int,
                "amount": _to_float,
                "latest_time": _to_datetime,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_index_hist records."""
        return self._cleaner.clean(raw_batch)


def _to_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip() == "":
        return None
    return float(value)


def _to_int(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip() == "":
        return None
    return int(float(value))


def _to_datetime(value: Any) -> Any:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value
