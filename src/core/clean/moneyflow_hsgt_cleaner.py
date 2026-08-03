from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch

_MILLION_TO_YUAN = Decimal("1000000")
_YUAN_SCALE = Decimal("0.01")


class MoneyflowHsgtCleaner:
    """Normalize Tushare moneyflow_hsgt rows into DB-ready records."""

    def __init__(self) -> None:
        optional_decimal = (Decimal, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "trade_date": "date",
                "ggt_ss": "ggt_ss",
                "ggt_sz": "ggt_sz",
                "hgt": "hgt",
                "sgt": "sgt",
                "north_money": "north_money",
                "south_money": "south_money",
            },
            type_map={
                "date": date,
                "ggt_ss": optional_decimal,
                "ggt_sz": optional_decimal,
                "hgt": optional_decimal,
                "sgt": optional_decimal,
                "north_money": optional_decimal,
                "south_money": optional_decimal,
            },
            required_fields={"date"},
            casts={
                "date": _parse_date,
                "ggt_ss": _as_yuan_decimal,
                "ggt_sz": _as_yuan_decimal,
                "hgt": _as_yuan_decimal,
                "sgt": _as_yuan_decimal,
                "north_money": _as_yuan_decimal,
                "south_money": _as_yuan_decimal,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw moneyflow_hsgt rows into DB-ready records."""
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


def _as_yuan_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return (value * _MILLION_TO_YUAN).quantize(_YUAN_SCALE)
    num = float(cast(Any, value))
    if math.isnan(num):
        return None
    return (Decimal(str(value)) * _MILLION_TO_YUAN).quantize(_YUAN_SCALE)
