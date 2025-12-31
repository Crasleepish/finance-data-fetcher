from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.clean.policy import ErrorMode
from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class FundInfoCleaner:
    """Normalize fund_basic rows into fund_info records."""

    def __init__(self) -> None:
        optional_str = (str, type(None))
        optional_float = (float, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "fund_code",
                "name": "fund_name",
                "fund_type": "fund_type",
                "invest_type": "invest_type",
                "found_date": "found_date",
                "m_fee": "fee_rate",
                "c_fee": "commission_rate",
                "market": "market",
            },
            type_map={
                "fund_code": str,
                "fund_name": str,
                "fund_type": optional_str,
                "invest_type": optional_str,
                "found_date": date,
                "fee_rate": optional_float,
                "commission_rate": optional_float,
                "market": optional_str,
            },
            required_fields={"fund_code", "fund_name", "found_date"},
            casts={
                "found_date": _parse_date,
                "fee_rate": _as_float,
                "commission_rate": _as_float,
            },
            error_mode=ErrorMode.DROP_RECORD,
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw fund_basic rows into fund_info records."""
        normalized = list(self._cleaner.clean(raw_batch))
        by_code: dict[str, dict[str, Any]] = {}
        for row in normalized:
            record = dict(row)
            code = record.get("fund_code")
            if isinstance(code, str) and code:
                by_code[code] = record
        return [by_code[code] for code in sorted(by_code.keys())]


def _parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        if not value:
            return None
        return datetime.strptime(value, "%Y%m%d").date()
    return value


def _as_float(value: Any) -> Any:
    if value is None:
        return None
    return float(value)
