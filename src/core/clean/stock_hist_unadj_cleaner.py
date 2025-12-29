from __future__ import annotations

from datetime import datetime
from typing import Any

from core.pipeline.types import NormalizedBatch, RawBatch


class StockHistUnadjCleaner:
    """Normalize daily + daily_basic + ST + suspend data into stock_hist_unadj rows."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Combine raw market data into DB-ready records."""
        if not raw_batch:
            return []
        payload = raw_batch[0]
        trade_date = _parse_trade_date(payload.get("trade_date"))
        daily_rows = _list_of_dicts(payload.get("daily"))
        daily_basic_rows = _list_of_dicts(payload.get("daily_basic"))
        suspend_rows = _list_of_dicts(payload.get("suspend"))

        daily_by_code = {
            str(row.get("ts_code")): row
            for row in daily_rows
            if isinstance(row.get("ts_code"), str) and row.get("ts_code")
        }
        basic_by_code = {
            str(row.get("ts_code")): row
            for row in daily_basic_rows
            if isinstance(row.get("ts_code"), str) and row.get("ts_code")
        }
        suspend_set = {
            str(row.get("ts_code"))
            for row in suspend_rows
            if isinstance(row.get("ts_code"), str) and row.get("ts_code")
        }

        codes = sorted({*daily_by_code.keys(), *suspend_set})
        records: list[dict[str, Any]] = []
        for code in codes:
            daily = daily_by_code.get(code, {})
            basic = basic_by_code.get(code, {})
            record = {
                "stock_code": code,
                "date": trade_date,
                "open": _as_float(daily.get("open")),
                "close": _as_float(_first_non_none(daily.get("close"), basic.get("close"))),
                "high": _as_float(daily.get("high")),
                "low": _as_float(daily.get("low")),
                "volume": _as_int(_scale(daily.get("vol"), 100)),
                "amount": _as_float(_scale(daily.get("amount"), 1000)),
                "pre_close": _as_float(daily.get("pre_close")),
                "change": _as_float(daily.get("change")),
                "change_percent": _as_float(daily.get("pct_chg")),
                "turnover_rate": _as_float(basic.get("turnover_rate")),
                "turnover_rate_f": _as_float(basic.get("turnover_rate_f")),
                "volume_ratio": _as_float(basic.get("volume_ratio")),
                "pe": _as_float(basic.get("pe")),
                "pe_ttm": _as_float(basic.get("pe_ttm")),
                "pb": _as_float(basic.get("pb")),
                "ps": _as_float(basic.get("ps")),
                "ps_ttm": _as_float(basic.get("ps_ttm")),
                "dv_ratio": _as_float(basic.get("dv_ratio")),
                "dv_ttm": _as_float(basic.get("dv_ttm")),
                "total_share": _as_int(_scale(basic.get("total_share"), 10000)),
                "float_share": _as_int(_scale(basic.get("float_share"), 10000)),
                "free_share": _as_int(_scale(basic.get("free_share"), 10000)),
                "mkt_cap": _as_int(_scale(basic.get("total_mv"), 10000)),
                "circ_mv": _as_int(_scale(basic.get("circ_mv"), 10000)),
                "is_suspend": "Y" if code in suspend_set else "N",
            }
            records.append(record)
        return records


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    raise ValueError("expected list of dicts")


def _parse_trade_date(value: Any) -> Any:
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


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
