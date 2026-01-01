from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.clean.policy import ErrorMode
from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class FundHistCleaner:
    """Normalize fund_nav rows into fund_hist records."""

    def __init__(self) -> None:
        optional_float = (float, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "fund_code",
                "nav_date": "date",
                "unit_nav": "value",
                "adj_nav": "net_value",
            },
            type_map={
                "fund_code": str,
                "date": date,
                "value": optional_float,
                "net_value": optional_float,
            },
            required_fields={"fund_code", "date"},
            casts={"date": _parse_date, "value": _as_float, "net_value": _as_float},
            error_mode=ErrorMode.DROP_RECORD,
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw fund_nav rows into fund_hist records."""
        normalized = list(self._cleaner.clean(raw_batch))
        by_key: dict[tuple[str, date], dict[str, Any]] = {}
        for row in normalized:
            record = dict(row)
            fund_code = record.get("fund_code")
            nav_date = record.get("date")
            if isinstance(fund_code, str) and isinstance(nav_date, date):
                by_key[(fund_code, nav_date)] = record
        return [by_key[key] for key in sorted(by_key.keys())]


def _parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        if len(value) == 10:
            return datetime.strptime(value, "%Y-%m-%d").date()
        return None
    return value


def _as_float(value: Any) -> Any:
    if value is None:
        return None
    return float(value)
