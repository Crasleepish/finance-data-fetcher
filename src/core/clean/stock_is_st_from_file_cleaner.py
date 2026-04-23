from __future__ import annotations

from datetime import datetime
from typing import Any

from core.pipeline.types import NormalizedBatch, RawBatch


class StockIsStFromFileCleaner:
    """Normalize file rows into stock_hist_unadj is_st updates."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, object]] = set()
        for row in raw_batch:
            stock_code = _normalize_stock_code(row.get("code"))
            trade_date = _parse_date(row.get("date"))
            is_st = _normalize_is_st(row.get("is_st"))
            key = (stock_code, trade_date)
            if key in seen:
                continue
            seen.add(key)
            records.append(
                {
                    "stock_code": stock_code,
                    "date": trade_date,
                    "is_st": is_st,
                }
            )
        return records


def _parse_date(value: Any) -> object:
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("date must be YYYY-MM-DD string")


def _normalize_stock_code(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("code must be string")
    code = value.strip().upper()
    if len(code) == 6 and code.isdigit():
        if code.startswith("6"):
            return f"{code}.SH"
        if code.startswith(("0", "3")):
            return f"{code}.SZ"
        if code.startswith(("4", "8", "9")):
            return f"{code}.BJ"
    if not code:
        raise ValueError("code is required")
    return code


def _normalize_is_st(value: Any) -> int:
    if not isinstance(value, str):
        raise ValueError("is_st must be string")
    normalized = value.strip()
    if normalized in {"是", "Y", "y", "1", "true", "True"}:
        return 1
    if normalized in {"否", "N", "n", "0", "false", "False"}:
        return 0
    raise ValueError(f"unsupported is_st value: {value}")
