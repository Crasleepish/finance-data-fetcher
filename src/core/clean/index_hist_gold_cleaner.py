from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from core.pipeline.types import NormalizedBatch, RawBatch


class IndexHistGoldCleaner:
    """Normalize Tushare sge_daily rows into index_hist records."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw sge_daily rows into DB-ready records."""
        records: list[dict[str, Any]] = []
        for row in _list_of_dicts(raw_batch):
            index_code = _as_str(row.get("index_code") or row.get("ts_code"))
            if not index_code:
                continue
            close = _as_float(row.get("close"))
            change = _as_float(row.get("change"))
            record = {
                "index_code": index_code,
                "date": _parse_date(row.get("trade_date")),
                "open": _fill_close(_as_float(row.get("open")), close),
                "close": close,
                "high": _fill_close(_as_float(row.get("high")), close),
                "low": _fill_close(_as_float(row.get("low")), close),
                "volume": _as_int(_scale(row.get("vol"), 1000)),
                "amount": _as_float(row.get("amount")),
                "change_percent": _compute_change_percent(change, close),
                "change": change,
            }
            records.append(record)
        return records


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    raise ValueError("expected list of dicts")


def _parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    return value


def _scale(value: Any, factor: int) -> Any:
    if value is None:
        return None
    num = float(value)
    if math.isnan(num):
        return None
    return num * factor


def _as_float(value: Any) -> Any:
    if value is None:
        return None
    num = float(value)
    if math.isnan(num):
        return None
    return num


def _as_int(value: Any) -> Any:
    if value is None:
        return None
    num = float(value)
    if math.isnan(num):
        return None
    return int(num)


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _fill_close(value: Any, close: Any) -> Any:
    return close if value is None else value


def _compute_change_percent(change: Any, close: Any) -> Any:
    if change is None or close is None:
        return None
    denom = close - change
    if denom == 0:
        return None
    return change / denom * 100
