from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class IndexDailyBasicCleaner:
    """Normalize Tushare index_dailybasic rows into DB-ready records."""

    def __init__(self) -> None:
        optional_decimal = (Decimal, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "ts_code",
                "trade_date": "trade_date",
                "total_mv": "total_mv",
                "float_mv": "float_mv",
                "total_share": "total_share",
                "float_share": "float_share",
                "free_share": "free_share",
                "turnover_rate": "turnover_rate",
                "turnover_rate_f": "turnover_rate_f",
                "pe": "pe",
                "pe_ttm": "pe_ttm",
                "pb": "pb",
            },
            type_map={
                "ts_code": str,
                "trade_date": date,
                "total_mv": optional_decimal,
                "float_mv": optional_decimal,
                "total_share": optional_decimal,
                "float_share": optional_decimal,
                "free_share": optional_decimal,
                "turnover_rate": optional_decimal,
                "turnover_rate_f": optional_decimal,
                "pe": optional_decimal,
                "pe_ttm": optional_decimal,
                "pb": optional_decimal,
            },
            required_fields={"ts_code", "trade_date"},
            casts={
                "trade_date": _parse_date,
                "total_mv": _as_decimal,
                "float_mv": _as_decimal,
                "total_share": _as_decimal,
                "float_share": _as_decimal,
                "free_share": _as_decimal,
                "turnover_rate": _as_decimal,
                "turnover_rate_f": _as_decimal,
                "pe": _as_decimal,
                "pe_ttm": _as_decimal,
                "pb": _as_decimal,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw index_dailybasic rows into DB-ready records."""
        return self._cleaner.clean(raw_batch)


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("expected YYYYMMDD or YYYY-MM-DD date string")


def _as_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    num = float(cast(Any, value))
    if math.isnan(num):
        return None
    return Decimal(str(value))
