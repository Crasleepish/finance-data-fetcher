from __future__ import annotations

from datetime import datetime
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class StockInfoCleaner:
    """Clean and normalize stock_basic rows into stock_info records."""

    def __init__(self) -> None:
        optional_str = (str, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "stock_code",
                "name": "stock_name",
                "market": "market",
                "exchange": "exchange",
                "industry": "industry",
                "list_date": "listing_date",
                "list_status": "list_status",
            },
            type_map={
                "stock_code": str,
                "stock_name": str,
                "market": optional_str,
                "exchange": optional_str,
                "industry": optional_str,
                "list_status": optional_str,
            },
            required_fields={"stock_code", "stock_name", "listing_date"},
            casts={"listing_date": _parse_date},
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw stock_basic rows into stock_info records."""
        return self._cleaner.clean(raw_batch)


def _parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return datetime.strptime(value, "%Y%m%d").date()
    return value
