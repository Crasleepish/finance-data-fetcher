from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class MarginDailyCleaner:
    """Normalize Tushare margin rows into DB-ready records."""

    def __init__(self) -> None:
        optional_decimal = (Decimal, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "trade_date": "trade_date",
                "exchange_id": "exchange_id",
                "rzye": "rzye",
                "rzmre": "rzmre",
                "rzche": "rzche",
                "rqye": "rqye",
                "rqmcl": "rqmcl",
                "rzrqye": "rzrqye",
                "rqyl": "rqyl",
            },
            type_map={
                "trade_date": date,
                "exchange_id": str,
                "rzye": optional_decimal,
                "rzmre": optional_decimal,
                "rzche": optional_decimal,
                "rqye": optional_decimal,
                "rqmcl": optional_decimal,
                "rzrqye": optional_decimal,
                "rqyl": optional_decimal,
            },
            required_fields={"trade_date", "exchange_id"},
            casts={
                "trade_date": _parse_date,
                "rzye": _as_decimal,
                "rzmre": _as_decimal,
                "rzche": _as_decimal,
                "rqye": _as_decimal,
                "rqmcl": _as_decimal,
                "rzrqye": _as_decimal,
                "rqyl": _as_decimal,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw margin rows into DB-ready records."""
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
