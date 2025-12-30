from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class AdjFactorCleaner:
    """Normalize Tushare adj_factor rows into adj_factor records."""

    def __init__(self) -> None:
        optional_float = (float, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "stock_code",
                "trade_date": "date",
                "adj_factor": "adj_factor",
            },
            type_map={
                "stock_code": str,
                "date": date,
                "adj_factor": optional_float,
            },
            required_fields={"stock_code", "date"},
            casts={"date": _parse_date, "adj_factor": _as_float},
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw adj_factor rows into DB-ready records."""
        return self._cleaner.clean(raw_batch)


def _parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    return value


def _as_float(value: Any) -> Any:
    if value is None:
        return None
    return float(value)
