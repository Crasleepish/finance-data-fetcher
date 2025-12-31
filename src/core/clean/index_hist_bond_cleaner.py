from __future__ import annotations

from datetime import datetime
from typing import Any

from core.pipeline.types import NormalizedBatch, RawBatch


class IndexHistBondCleaner:
    """Normalize Akshare csindex rows into index_hist records."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw csindex rows into DB-ready records."""
        records: list[dict[str, Any]] = []
        for row in _list_of_dicts(raw_batch):
            index_code = _as_str(row.get("index_code") or row.get("指数代码"))
            if not index_code:
                continue
            close = _as_float(row.get("收盘"))
            record = {
                "index_code": index_code,
                "date": _parse_date(row.get("日期")),
                "open": _fill_close(_as_float(row.get("开盘")), close),
                "close": close,
                "high": _fill_close(_as_float(row.get("最高")), close),
                "low": _fill_close(_as_float(row.get("最低")), close),
                "volume": _as_int(_scale(row.get("成交量"), 1_000_000)),
                "amount": _as_float(_scale(row.get("成交金额"), 100_000_000)),
                "change_percent": _as_float(row.get("涨跌幅")),
                "change": _as_float(row.get("涨跌")),
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
    return float(value) * factor


def _as_float(value: Any) -> Any:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> Any:
    if value is None:
        return None
    return int(float(value))


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _fill_close(value: Any, close: Any) -> Any:
    return close if value is None else value
