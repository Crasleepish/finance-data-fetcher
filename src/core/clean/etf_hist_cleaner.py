from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class EtfHistCleaner:
    """Normalize fund_daily rows into etf_hist records."""

    def __init__(self) -> None:
        optional_float = (float, type(None))
        optional_int = (int, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "etf_code",
                "trade_date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "change": "change",
                "pct_chg": "change_percent",
                "vol": "volume",
                "amount": "amount",
            },
            type_map={
                "etf_code": str,
                "date": date,
                "open": optional_float,
                "high": optional_float,
                "low": optional_float,
                "close": optional_float,
                "change": optional_float,
                "change_percent": optional_float,
                "volume": optional_int,
                "amount": optional_float,
            },
            required_fields={"etf_code", "date"},
            casts={
                "date": _parse_date,
                "open": _as_float,
                "high": _as_float,
                "low": _as_float,
                "close": _as_float,
                "change": _as_float,
                "change_percent": _as_float,
                "volume": _as_int_shares,
                "amount": _as_amount,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw fund_daily rows into etf_hist records."""
        return self._cleaner.clean(raw_batch)


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


def _as_int_shares(value: Any) -> Any:
    if value is None:
        return None
    return int(float(value) * 100)


def _as_amount(value: Any) -> Any:
    if value is None:
        return None
    return float(value) * 1000
