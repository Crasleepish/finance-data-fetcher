from __future__ import annotations

from datetime import datetime
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class RtMarketFactorsCleaner:
    """Clean and normalize real-time market_factors snapshots."""

    def __init__(self) -> None:
        optional_float = (float, int, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "latest_date": "latest_date",
                "MKT": "MKT",
                "SMB": "SMB",
                "HML": "HML",
                "QMJ": "QMJ",
            },
            type_map={
                "latest_date": datetime,
                "MKT": optional_float,
                "SMB": optional_float,
                "HML": optional_float,
                "QMJ": optional_float,
            },
            required_fields={"latest_date"},
            casts={
                "MKT": _to_float,
                "SMB": _to_float,
                "HML": _to_float,
                "QMJ": _to_float,
                "latest_date": _to_datetime,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_market_factors records."""
        return self._cleaner.clean(raw_batch)


def _to_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip() == "":
        return None
    return float(value)


def _to_datetime(value: Any) -> Any:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value
