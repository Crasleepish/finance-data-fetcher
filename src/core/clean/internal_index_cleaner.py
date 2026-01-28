from __future__ import annotations

from datetime import date
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class InternalIndexCleaner:
    """Normalize internal index rows for index_hist persistence."""

    def __init__(self) -> None:
        optional_float = (float, int, type(None))
        optional_int = (int, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "index_code": "index_code",
                "date": "date",
                "open": "open",
                "close": "close",
                "high": "high",
                "low": "low",
                "volume": "volume",
                "amount": "amount",
                "change_percent": "change_percent",
                "change": "change",
            },
            type_map={
                "index_code": str,
                "date": date,
                "open": optional_float,
                "close": optional_float,
                "high": optional_float,
                "low": optional_float,
                "volume": optional_int,
                "amount": optional_float,
                "change_percent": optional_float,
                "change": optional_float,
            },
            required_fields={"index_code", "date", "close"},
            casts={
                "open": _to_float,
                "close": _to_float,
                "high": _to_float,
                "low": _to_float,
                "volume": _to_int,
                "amount": _to_float,
                "change_percent": _to_float,
                "change": _to_float,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw internal index rows into index_hist records."""
        return self._cleaner.clean(raw_batch)


def _to_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return float(value)


def _to_int(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return int(float(value))
