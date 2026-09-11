from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, cast

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch

# sz_daily_info source category (Chinese) -> (target ts_code, target ts_name).
# Consistent with the historical daily_info key style. An exhaustive source
# scan (2008-01-02..2025-12-31) found 23 categories; all are mapped below.
# Only legacy SZ_MAIN has no source equivalent (the closest sources 股票 /
# 主板A股 map to SZ_MARKET / SZ_A). Note that 权证 and 股票权证 are duplicate
# source labels with identical values (both map to SZ_WR); clean() de-duplicates
# identical normalized (trade_date, ts_code) records and raises on conflicts.
_SZ_CATEGORY_TO_TARGET: dict[str, tuple[str, str]] = {
    "股票": ("SZ_MARKET", "深圳市场"),
    "主板A股": ("SZ_A", "深圳主板A股"),
    "主板B股": ("SZ_B", "深圳B股"),
    "创业板A股": ("SZ_GEM_A", "创业板A股"),
    "创业板": ("SZ_GEM", "创业板"),
    "中小板": ("SZ_SME", "中小企业板"),
    "基金": ("SZ_FUND", "深圳基金市场"),
    "ETF": ("SZ_FUND_ETF", "深圳基金ETF"),
    "LOF": ("SZ_FUND_LOF", "深圳基金LOF"),
    "封闭式基金": ("SZ_FUND_CEF", "深圳封闭基金"),
    "分级基金": ("SZ_FUND_SF", "深圳分级基金"),
    "基础设施基金": ("SZ_FUND_REIT", "深圳基础设施基金"),
    "债券": ("SZ_BOND", "深圳债券"),
    "债券现券": ("SZ_BOND_CN", "深圳债券现券"),
    "债券回购": ("SZ_BOND_REP", "深圳债券回购"),
    "企业债": ("SZ_BOND_ENT", "深圳企业债"),
    "公司债": ("SZ_BOND_COR", "深圳公司债"),
    "国债": ("SZ_BOND_GOV", "深圳国债"),
    "可转换债券": ("SZ_BOND_CB", "深圳可转债"),
    "ABS": ("SZ_BOND_ABS", "深圳债券ABS"),
    "期权": ("SZ_OPTION", "深圳期权"),
    "权证": ("SZ_WR", "深圳权证"),
    "股票权证": ("SZ_WR", "深圳权证"),
}

# sz_daily_info raw financial fields (amount, vol, total_share, total_mv,
# float_share, float_mv) are in yuan/shares; the table stores 亿元/亿股, so
# values are divided by 1e8 during normalization.
_SZ_SCALE = Decimal("1e8")


class MarketDailyInfoCleaner:
    """Normalize Tushare 市场交易统计 rows into DB-ready records.

    SH rows (tagged `_source: "sh"` by the SH fetcher, or untagged) retain the
    standard 14-field daily_info path. SZ rows (tagged `_source: "sz"`) come
    from pro.sz_daily_info and are remapped here: the Chinese category ts_code
    is translated to the target ts_code/ts_name, `count` maps to `com_count`,
    yuan/shares financial fields are scaled to 亿元/亿股, `exchange` is set to
    "SZ", and trans_count/pe/tr are set to NULL. Source NULL/NaN is preserved
    as NULL; an unknown source category fails loudly.

    `ts_name` is a display-only field (not part of the primary key) and may be
    absent/null. `trans_count` is documented as an int but the API can return
    floats, so it is stored as a decimal. `tr` (换手率) is null for SZ boards.

    The SZ source labels 权证 / 股票权证 are duplicates with identical values
    and both map to the target PK (trade_date, SZ_WR). clean() therefore
    de-duplicates normalized records by (trade_date, ts_code): the first record
    is kept, an exactly equal later record is dropped (output order preserved),
    and a conflicting later record raises instead of silently last-write-wins.
    This applies to all normalized rows, not only the two warrant labels.
    """

    def __init__(self) -> None:
        optional_decimal = (Decimal, type(None))
        optional_int = (int, type(None))
        optional_str = (str, type(None))
        self._cleaner = TypedCleaner(
            field_map={
                "trade_date": "trade_date",
                "ts_code": "ts_code",
                "ts_name": "ts_name",
                "com_count": "com_count",
                "total_share": "total_share",
                "float_share": "float_share",
                "total_mv": "total_mv",
                "float_mv": "float_mv",
                "amount": "amount",
                "vol": "vol",
                "trans_count": "trans_count",
                "pe": "pe",
                "tr": "tr",
                "exchange": "exchange",
            },
            type_map={
                "trade_date": date,
                "ts_code": str,
                "ts_name": optional_str,
                "com_count": optional_int,
                "total_share": optional_decimal,
                "float_share": optional_decimal,
                "total_mv": optional_decimal,
                "float_mv": optional_decimal,
                "amount": optional_decimal,
                "vol": optional_decimal,
                "trans_count": optional_decimal,
                "pe": optional_decimal,
                "tr": optional_decimal,
                "exchange": optional_str,
            },
            required_fields={"trade_date", "ts_code"},
            casts={
                "trade_date": _parse_date,
                "com_count": _as_int,
                "total_share": _as_decimal,
                "float_share": _as_decimal,
                "total_mv": _as_decimal,
                "float_mv": _as_decimal,
                "amount": _as_decimal,
                "vol": _as_decimal,
                "trans_count": _as_decimal,
                "pe": _as_decimal,
                "tr": _as_decimal,
            },
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw daily_info rows (SH or SZ) into DB-ready records."""
        records = [self._to_target_record(raw) for raw in raw_batch]
        normalized = self._cleaner.clean(records)
        return self._deduplicate(normalized)

    def _deduplicate(self, normalized: NormalizedBatch) -> NormalizedBatch:
        """Drop exact duplicate normalized records with the same (trade_date, ts_code).

        The SZ source can yield the same target row under two different source
        labels (权证 / 股票权证 both map to SZ_WR). The first occurrence is
        kept; an exactly equal later record is dropped; a conflicting later
        record raises instead of silently last-write-wins. Output order is
        preserved.
        """
        seen: dict[tuple[object, object], Mapping[str, Any]] = {}
        deduplicated: list[Mapping[str, Any]] = []
        for record in normalized:
            key = (record["trade_date"], record["ts_code"])
            previous = seen.get(key)
            if previous is None:
                seen[key] = record
                deduplicated.append(record)
            elif previous != record:
                raise ValueError(
                    "conflicting normalized records for "
                    f"(trade_date={record['trade_date']}, ts_code={record['ts_code']})"
                )
        return deduplicated

    def _to_target_record(self, raw: Any) -> Any:
        if isinstance(raw, dict):
            if raw.get("_source") == "sz":
                return self._normalize_sz_record(raw)
            return {key: value for key, value in raw.items() if key != "_source"}
        return raw

    def _normalize_sz_record(self, raw: dict[str, Any]) -> dict[str, Any]:
        category = raw.get("ts_code")
        target = _SZ_CATEGORY_TO_TARGET.get(category) if isinstance(category, str) else None
        if target is None:
            raise ValueError(f"unknown sz_daily_info category: {category!r}")
        target_ts_code, target_ts_name = target
        return {
            "trade_date": raw.get("trade_date"),
            "ts_code": target_ts_code,
            "ts_name": target_ts_name,
            "com_count": _nan_to_none(raw.get("count")),
            "total_share": _scale_from_yuan(raw.get("total_share")),
            "float_share": _scale_from_yuan(raw.get("float_share")),
            "total_mv": _scale_from_yuan(raw.get("total_mv")),
            "float_mv": _scale_from_yuan(raw.get("float_mv")),
            "amount": _scale_from_yuan(raw.get("amount")),
            "vol": _scale_from_yuan(raw.get("vol")),
            "trans_count": None,
            "pe": None,
            "tr": None,
            "exchange": "SZ",
        }


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


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("expected integer-like value")
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        raise ValueError("expected integer-like value")
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return int(value)
        raise ValueError("expected integer-like value")
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    raise ValueError("expected integer-like value")


def _nan_to_none(value: object) -> object:
    """Return None for NULL/NaN, otherwise the value unchanged."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return None if value.is_nan() else value
    try:
        num = float(cast(Any, value))
    except (TypeError, ValueError):
        return value
    return None if math.isnan(num) else value


def _scale_from_yuan(value: object) -> Decimal | None:
    """Scale a raw yuan/shares value to 亿元/亿股, preserving NULL/NaN as NULL."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value.is_nan():
            return None
        return value / _SZ_SCALE
    num = float(cast(Any, value))
    if math.isnan(num):
        return None
    return Decimal(str(value)) / _SZ_SCALE
