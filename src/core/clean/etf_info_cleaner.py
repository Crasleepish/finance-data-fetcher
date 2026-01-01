from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class EtfInfoCleaner:
    """Normalize fund_basic rows into etf_info records."""

    def __init__(self) -> None:
        optional_str = (str, type(None))
        optional_date = (date, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "etf_code",
                "name": "etf_name",
                "fund_type": "fund_type",
                "invest_type": "invest_type",
                "found_date": "found_date",
            },
            type_map={
                "etf_code": str,
                "etf_name": str,
                "fund_type": optional_str,
                "invest_type": optional_str,
                "found_date": optional_date,
            },
            required_fields={"etf_code", "etf_name"},
            casts={"found_date": _parse_date},
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw fund_basic rows into etf_info records."""
        return self._cleaner.clean(raw_batch)


def _parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        if not value:
            return None
        return datetime.strptime(value, "%Y%m%d").date()
    return value
