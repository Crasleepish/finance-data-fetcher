from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from core.pipeline.types import NormalizedBatch, RawBatch

logger = logging.getLogger(__name__)

SHARE_SOURCE = "share"
SEED_SOURCE = "seed"
NAV_SOURCE = "nav"

_TEN_THOUSAND = Decimal("10000")
_AUM_10K_SCALE = Decimal("0.0001")
_AUM_SCALE = Decimal("0.01")


@dataclass(frozen=True)
class _NavCandidate:
    """A single fund_nav row for one code on the chunk's nav date."""

    ts_code: str
    nav_date: date
    ann_date: date | None
    unit_nav: Decimal | None


@dataclass(frozen=True)
class _ShareValue:
    """A resolved fd_share value and the trade date that produced it."""

    source_date: date
    fd_share: Decimal


class EtfDailySizeCleaner:
    """Normalize one trade day of fund_share/fund_nav rows into etf_daily_size rows.

    Raw records are tagged with `_source`:
    - "share": fund_share event on the chunk trade date. A valid value overrides
      seeds; rows whose fd_share is missing/NaN/Infinity are treated as absent so
      DB seeds or backfilled history still apply (and never block the chunk);
    - "seed": a fund_share value carried forward from an earlier trade date;
    - "nav": a fund_nav candidate on the chunk nav date.

    The output universe is the union of codes with a valid share (same-day or
    seeded) only. fund_nav is authoritative for nothing but NAV values: NAV
    rows are left-joined onto that universe, and NAV-only codes (the
    fund_nav market E feed also carries non-ETF funds) never produce records
    and never widen the universe. Codes without any NAV row on the chunk date
    still produce a record with `nav_missing=True` and NULL NAV/AUM columns.
    NAV is never forward-filled and shares are never defaulted to 0.
    Same-(ts_code, trade_date) conflicts raise, while identical duplicates
    collapse. Dates/numbers that are NaN, NaT or non-finite are treated as
    empty and never reach the database or arithmetic.
    """

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw fund_share/fund_nav records into etf_daily_size rows."""
        shares: dict[str, dict[date, set[Decimal]]] = {}
        seeds: dict[str, dict[date, set[Decimal]]] = {}
        nav_rows: list[_NavCandidate] = []
        chunk_dates: set[date] = set()
        for raw in raw_batch:
            if not isinstance(raw, Mapping):
                raise ValueError("record must be a mapping")
            tagged = _optional_date(raw.get("_chunk_date"))
            if tagged is not None:
                chunk_dates.add(tagged)
            source = raw.get("_source")
            if source == SHARE_SOURCE:
                _collect_share(shares, raw, kind="fund_share")
            elif source == SEED_SOURCE:
                _collect_share(seeds, raw, kind="fund_share seed")
            elif source == NAV_SOURCE:
                nav_rows.append(_parse_nav(raw))
            else:
                raise ValueError(f"unsupported record source: {source!r}")

        if not shares and not seeds:
            return []

        trade_date = _resolve_trade_date(chunk_dates, shares, seeds, nav_rows)

        nav_by_code: dict[str, list[_NavCandidate]] = {}
        for candidate in nav_rows:
            nav_by_code.setdefault(candidate.ts_code, []).append(candidate)

        records: list[dict[str, object]] = []
        skipped: list[str] = []
        for ts_code in sorted(set(shares) | set(seeds)):
            share = _resolve_share_value(ts_code, shares.get(ts_code, {}))
            if share is None:
                share = _resolve_share_value(ts_code, seeds.get(ts_code, {}))
            if share is None:
                skipped.append(ts_code)
                continue
            candidates = nav_by_code.get(ts_code, [])
            selected = _select_nav_version(candidates) if candidates else None
            records.append(_build_record(trade_date, ts_code, share, selected))

        if skipped:
            logger.warning(
                "etf daily size skipped codes without valid fund_share",
                extra={"trade_date": trade_date.isoformat(), "codes": skipped},
            )
        logger.info(
            "etf daily size clean completed",
            extra={
                "trade_date": trade_date.isoformat(),
                "raw_count": len(raw_batch),
                "record_count": len(records),
                "skipped_count": len(skipped),
            },
        )
        return records


def _collect_share(
    store: dict[str, dict[date, set[Decimal]]], raw: Mapping[str, Any], *, kind: str
) -> None:
    """Collect one tagged share/seed row, keeping only finite fd_share values."""
    ts_code = _require_ts_code(raw)
    source_date = _require_date(raw.get("trade_date"), "trade_date")
    fd_share = optional_decimal(raw.get("fd_share"))
    values = store.setdefault(ts_code, {}).setdefault(source_date, set())
    if fd_share is None:
        logger.debug(
            "etf daily size ignoring non-finite fd_share",
            extra={"ts_code": ts_code, "source_date": source_date.isoformat(), "kind": kind},
        )
        return
    values.add(fd_share)


def _resolve_share_value(ts_code: str, per_code: dict[date, set[Decimal]]) -> _ShareValue | None:
    """Resolve one code's share from the latest source date with a valid value.

    Identical duplicates collapse; multiple distinct valid values on the same
    (ts_code, trade_date) key are a hard conflict.
    """
    for source_date in sorted(per_code, reverse=True):
        values = per_code[source_date]
        if len(values) > 1:
            raise ValueError(
                f"conflicting fd_share for {ts_code} on {source_date.isoformat()}: "
                f"{sorted(values, key=str)}"
            )
        if values:
            return _ShareValue(source_date=source_date, fd_share=next(iter(values)))
    return None


def _resolve_trade_date(
    chunk_dates: set[date],
    shares: dict[str, dict[date, set[Decimal]]],
    seeds: dict[str, dict[date, set[Decimal]]],
    nav_rows: list[_NavCandidate],
) -> date:
    """Resolve the chunk trade date D and validate all inputs against it."""
    if len(chunk_dates) > 1:
        raise ValueError("input rows span multiple chunk dates")
    if chunk_dates:
        trade_date = next(iter(chunk_dates))
    else:
        nav_dates = {candidate.nav_date for candidate in nav_rows}
        if len(nav_dates) > 1:
            raise ValueError("fund_nav rows span multiple nav dates")
        if nav_dates:
            trade_date = next(iter(nav_dates))
        else:
            share_dates = {day for per_code in shares.values() for day in per_code}
            if len(share_dates) != 1:
                raise ValueError("chunk trade date cannot be resolved from input rows")
            trade_date = next(iter(share_dates))

    nav_dates = {candidate.nav_date for candidate in nav_rows}
    if nav_dates and nav_dates != {trade_date}:
        raise ValueError("fund_nav nav_date does not match the chunk trade date")
    share_dates = {day for per_code in shares.values() for day in per_code}
    if share_dates and share_dates != {trade_date}:
        raise ValueError("fund_share trade_date does not match the chunk trade date")
    for per_code in seeds.values():
        for day in per_code:
            if day > trade_date:
                raise ValueError(
                    "seed fund_share is dated after the chunk trade date "
                    f"({day.isoformat()} > {trade_date.isoformat()})"
                )
    return trade_date


def _parse_nav(raw: Mapping[str, Any]) -> _NavCandidate:
    """Parse a tagged fund_nav record, ignoring unknown Tushare columns."""
    return _NavCandidate(
        ts_code=_require_ts_code(raw),
        nav_date=_require_date(raw.get("nav_date"), "nav_date"),
        ann_date=_optional_date(raw.get("ann_date")),
        unit_nav=optional_decimal(raw.get("unit_nav")),
    )


def _select_nav_version(candidates: list[_NavCandidate]) -> _NavCandidate:
    """Select one NAV version without relying on API row order.

    Non-null ann_date wins over null; the maximum ann_date is selected. Within
    the selected version, differing unit_nav values are ambiguous and raise.
    A selected version with null unit_nav is kept (nav_missing), never replaced
    by an older version.
    """
    ts_code = candidates[0].ts_code
    dated = [candidate for candidate in candidates if candidate.ann_date is not None]
    considered = candidates
    if dated:
        ann_dates = [candidate.ann_date for candidate in dated if candidate.ann_date is not None]
        latest_ann = max(ann_dates)
        considered = [candidate for candidate in dated if candidate.ann_date == latest_ann]
    distinct = {(candidate.unit_nav is None, candidate.unit_nav) for candidate in considered}
    if len(distinct) > 1:
        raise ValueError(
            f"conflicting unit_nav for {ts_code} on "
            f"{candidates[0].nav_date.isoformat()}: {sorted(distinct, key=repr)}"
        )
    return considered[0]


def _build_record(
    trade_date: date,
    ts_code: str,
    share: _ShareValue,
    selected: _NavCandidate | None,
) -> dict[str, object]:
    """Build one etf_daily_size record, computing AUM strictly in Decimal.

    AUM is derived from the unrounded raw product: aum_10k_cny quantizes the raw
    product to 0.0001 and aum_cny independently quantizes raw * 10000 to 0.01,
    avoiding precision loss from multiplying the rounded 万元 value.
    """
    record: dict[str, object] = {
        "trade_date": trade_date,
        "ts_code": ts_code,
        "fd_share": share.fd_share,
        "share_source_date": share.source_date,
        "nav_date": trade_date if selected is None else selected.nav_date,
        "ann_date": None if selected is None else selected.ann_date,
        "unit_nav": None,
        "aum_10k_cny": None,
        "aum_cny": None,
        "nav_missing": selected is None or selected.unit_nav is None,
    }
    if selected is None or selected.unit_nav is None:
        return record

    raw_aum = share.fd_share * selected.unit_nav
    record["unit_nav"] = selected.unit_nav
    record["aum_10k_cny"] = raw_aum.quantize(_AUM_10K_SCALE)
    record["aum_cny"] = (raw_aum * _TEN_THOUSAND).quantize(_AUM_SCALE)
    return record


def optional_decimal(value: object) -> Decimal | None:
    """Return a finite Decimal, or None for missing/NaN/Infinity/unparseable values."""
    if _is_missing(value):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _is_missing(value: object) -> bool:
    """Detect None/NaN/NaT-like missing values without importing pandas."""
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def _require_ts_code(raw: Mapping[str, Any]) -> str:
    ts_code = raw.get("ts_code")
    if not isinstance(ts_code, str) or not ts_code:
        raise ValueError("missing ts_code")
    return ts_code


def _require_date(value: object, field: str) -> date:
    parsed = _optional_date(value)
    if parsed is None:
        raise ValueError(f"missing {field}")
    return parsed


def _optional_date(value: object) -> date | None:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"invalid date: {value!r}")
