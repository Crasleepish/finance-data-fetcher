from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any

from core.pipeline.types import NormalizedBatch, RawBatch

_SUFFIXED_CODE_RE = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
_BARE_CODE_RE = re.compile(r"^[0-9]{6}$")
_SUFFIX_RE = re.compile(r"\.(SH|SZ|BJ)$")


class StockCodeChangeCleaner:
    """Normalize raw stock code-change rows and validate interval consistency.

    The snapshot is global: every raw row is validated in the context of all
    other rows, so a failed validation aborts the whole snapshot rather than
    dropping a single record.
    """

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw rows into a validated, deduplicated snapshot."""
        normalized = [_normalize_record(raw) for raw in raw_batch]
        records = _dedupe(normalized)
        _validate_chains(records)
        return records


def _normalize_record(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError("record must be a mapping")

    trade_code_raw = raw.get("trade_code")
    if trade_code_raw is None or not str(trade_code_raw).strip():
        raise ValueError("missing required field: trade_code")
    trade_code = _require_suffixed(str(trade_code_raw).strip(), "trade_code")

    name_raw = raw.get("name")
    if name_raw is None or not str(name_raw).strip():
        raise ValueError("missing required field: name")
    name = str(name_raw).strip()

    suffix_match = _SUFFIX_RE.search(trade_code)
    assert suffix_match is not None  # guaranteed by _require_suffixed
    market = suffix_match.group(1)

    code_raw = raw.get("code")
    if code_raw is None or not str(code_raw).strip():
        raise ValueError("missing required field: code")
    code = _normalize_code(str(code_raw).strip(), market)

    start_date = _parse_date(raw.get("start_date"), "start_date", required=True)
    end_date = _parse_date(raw.get("end_date"), "end_date", required=False)
    assert start_date is not None  # required
    if end_date is not None and end_date < start_date:
        raise ValueError(f"end_date before start_date for {trade_code}/{code}")

    return {
        "trade_code": trade_code,
        "code": code,
        "name": name,
        "start_date": start_date,
        "end_date": end_date,
    }


def _require_suffixed(value: str, field: str) -> str:
    if not _SUFFIXED_CODE_RE.match(value):
        raise ValueError(f"invalid {field}, expected ^[0-9]{{6}}\\.(SH|SZ|BJ)$: {value!r}")
    return value


def _normalize_code(value: str, market: str) -> str:
    """Attach the latest market suffix, or verify an already-suffixed code."""
    if _SUFFIXED_CODE_RE.match(value):
        suffix_match = _SUFFIX_RE.search(value)
        assert suffix_match is not None
        if suffix_match.group(1) != market:
            raise ValueError(
                f"code suffix {suffix_match.group(1)} does not match "
                f"latest market {market}: {value!r}"
            )
        return value
    if _BARE_CODE_RE.match(value):
        return f"{value}.{market}"
    raise ValueError(f"invalid historical code, expected 6 digits or suffixed code: {value!r}")


def _parse_date(value: Any, field: str, *, required: bool) -> date | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise ValueError(f"missing required field: {field}")
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"invalid date for {field}: {value!r}")


def _dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, date], dict[str, Any]] = {}
    for record in records:
        key = (record["trade_code"], record["code"], record["start_date"])
        existing = deduped.get(key)
        if existing is not None:
            if existing != record:
                raise ValueError(f"conflicting duplicate record for key {key}")
            continue
        deduped[key] = record
    if not deduped:
        raise ValueError("empty stock code change snapshot")
    return list(deduped.values())


def _validate_chains(records: list[dict[str, Any]]) -> None:
    by_trade: dict[str, list[dict[str, Any]]] = {}
    ownership_map: dict[str, set[str]] = {}
    for record in records:
        by_trade.setdefault(record["trade_code"], []).append(record)
        ownership_map.setdefault(record["code"], set()).add(record["trade_code"])

    ambiguous = {code: trades for code, trades in ownership_map.items() if len(trades) > 1}
    if ambiguous:
        detail = {code: sorted(trades) for code, trades in sorted(ambiguous.items())}
        raise ValueError(f"ambiguous historical code mapping to multiple latest codes: {detail}")

    for trade_code, items in by_trade.items():
        items.sort(key=lambda item: item["start_date"])
        open_intervals = [item for item in items if item["end_date"] is None]
        if len(open_intervals) != 1:
            raise ValueError(
                f"{trade_code}: expected exactly one open interval, found {len(open_intervals)}"
            )
        for index in range(1, len(items)):
            previous = items[index - 1]
            current = items[index]
            if previous["end_date"] is None:
                raise ValueError(f"{trade_code}: open interval is not the last interval")
            if current["start_date"] <= previous["end_date"]:
                raise ValueError(f"{trade_code}: overlapping intervals")
            if current["start_date"] != previous["end_date"] + timedelta(days=1):
                raise ValueError(f"{trade_code}: gap between intervals")
        if open_intervals[0]["code"] != trade_code:
            raise ValueError(f"{trade_code}: open interval code must equal latest trade_code")
        latest_name = open_intervals[0]["name"]
        for item in items:
            if item["name"] != latest_name:
                raise ValueError(
                    f"{trade_code}: name {item['name']!r} does not match latest name "
                    f"{latest_name!r}"
                )
