from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import TypedDict

from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine

from core.clean.rt_index_hist_cleaner import RtIndexHistCleaner
from core.indexing.index_codes import IndexCodeMapping, build_code_mappings, parse_index_codes
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.db.tables import index_info, rt_index_hist
from infra.fetcher.akshare_index_hist_min_fetcher import AkshareIndexHistMinFetcher
from infra.fetcher.pysnowball_quotec_fetcher import PysnowballQuotecFetcher
from infra.index_catalog import require_index_codes
from infra.xueqiu_token_fetcher import XueqiuTokenFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RtIndexHistXueqiuPipeline(IngestionPipeline):
    """Real-time index snapshot pipeline via pysnowball."""

    engine: Engine
    rt_fetch_interval_s: int
    codes_raw: str
    _fetcher: PysnowballQuotecFetcher = field(init=False, repr=False)
    _cleaner: RtIndexHistCleaner = field(init=False, repr=False)
    _codes: list[IndexCodeMapping] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        codes = require_index_codes(self.engine, index_info, parse_index_codes(self.codes_raw))
        mappings = build_code_mappings(codes, _to_xueqiu_code)
        object.__setattr__(self, "_codes", mappings)
        object.__setattr__(
            self,
            "_fetcher",
            PysnowballQuotecFetcher(
                token_fetcher=XueqiuTokenFetcher(),
                codes=mappings,
            ),
        )
        object.__setattr__(self, "_cleaner", RtIndexHistCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not self._codes:
            return []
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [{"params": {}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index snapshot rows."""
        rows = self._fetcher.fetch(chunk_args)
        if not rows:
            raise RuntimeError("pysnowball returned empty snapshot data")
        latest_time = datetime.now()
        return [{**row, "latest_time": latest_time} for row in rows]

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_index_hist records."""
        return self._cleaner.clean(raw_batch)


@dataclass(frozen=True)
class RtIndexHistAksharePipeline(IngestionPipeline):
    """Real-time index snapshot pipeline via Akshare index min data."""

    engine: Engine
    rt_fetch_interval_s: int
    fetcher: AkshareIndexHistMinFetcher
    codes_raw: str
    _cleaner: RtIndexHistCleaner = field(init=False, repr=False)
    _codes: list[IndexCodeMapping] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        codes = require_index_codes(self.engine, index_info, parse_index_codes(self.codes_raw))
        mappings = build_code_mappings(codes, _to_akshare_code)
        object.__setattr__(self, "_codes", mappings)
        object.__setattr__(self, "_cleaner", RtIndexHistCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not self._codes:
            return []
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [
            {"params": {"index_code": mapping["index_code"], "api_code": mapping["api_code"]}}
            for mapping in self._codes
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index snapshot rows."""
        params = chunk_args.get("params") or {}
        index_code = params.get("index_code")
        api_code = params.get("api_code")
        if not isinstance(index_code, str) or not isinstance(api_code, str):
            raise ValueError("missing index_code/api_code")
        prev_trade_day = _prev_trade_day(self.engine)
        if prev_trade_day is None:
            raise RuntimeError("previous trade day not found")
        start_dt = datetime.combine(prev_trade_day, time(15, 0, 0))
        end_dt = datetime.now()
        rows = self.fetcher.fetch(
            {
                "params": {
                    "code": api_code,
                    "start_date": _format_ts(start_dt),
                    "end_date": _format_ts(end_dt),
                }
            }
        )
        if not rows:
            raise RuntimeError("akshare index min returned empty snapshot data")
        snapshot = _build_snapshot_from_min(rows, index_code, end_dt)
        return [snapshot]

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_index_hist records."""
        return self._cleaner.clean(raw_batch)


def _should_fetch(engine: Engine, interval_s: int) -> bool:
    with engine.begin() as connection:
        latest = connection.execute(select(func.max(rt_index_hist.c.latest_time))).scalar()
    if latest is None:
        return True
    elapsed = (datetime.now() - latest).total_seconds()
    if elapsed < interval_s:
        logger.info(
            "rt index snapshot skipped due to fetch interval",
            extra={"elapsed_s": int(elapsed), "interval_s": interval_s},
        )
        return False
    return True


def _to_xueqiu_code(index_code: str) -> str:
    if "." not in index_code:
        return index_code
    base, suffix = index_code.split(".", 1)
    return f"{suffix}{base}"


def _to_akshare_code(index_code: str) -> str:
    if "." in index_code:
        return index_code.split(".", 1)[0]
    return index_code


def _prev_trade_day(engine: Engine) -> date | None:
    with engine.begin() as connection:
        return connection.execute(
            text("select max(date) from trade_calendar where date < current_date")
        ).scalar()


def _format_ts(value: datetime) -> str:
    return value.strftime("%Y%m%d%H%M%S")


def _build_snapshot_from_min(
    rows: RawBatch,
    index_code: str,
    latest_time: datetime,
) -> dict[str, object]:
    parsed: list[_MinRow] = []
    for row in rows:
        if isinstance(row, dict):
            parsed_row = _parse_min_row(row)
            if parsed_row is not None:
                parsed.append(parsed_row)
    if not parsed:
        raise RuntimeError("akshare min rows missing required fields")
    parsed.sort(key=lambda item: item["ts"])
    earliest = parsed[0]
    rest = parsed[1:]
    pre_close = earliest["close"]
    close = rest[-1]["close"] if rest else earliest["close"]
    open_price = rest[0]["open"] if rest else None
    high = max((item["high"] for item in rest), default=None)
    low = min((item["low"] for item in rest), default=None)
    volume = sum((item["volume"] or 0 for item in rest), 0) if rest else None
    amount = sum((item["amount"] or 0.0 for item in rest), 0.0) if rest else None
    return {
        "index_code": index_code,
        "open": open_price,
        "close": close,
        "high": high,
        "low": low,
        "pre_close": pre_close,
        "volume": volume,
        "amount": amount,
        "latest_time": latest_time,
    }


def _parse_min_row(row: dict[str, object]) -> _MinRow | None:
    ts_raw = row.get("时间") or row.get("time")
    ts = _parse_ts(ts_raw)
    if ts is None:
        return None
    open_price = _to_float(row.get("开盘") or row.get("open"))
    close = _to_float(row.get("收盘") or row.get("close"))
    high = _to_float(row.get("最高") or row.get("high"))
    low = _to_float(row.get("最低") or row.get("low"))
    if open_price is None or close is None or high is None or low is None:
        return None
    return {
        "ts": ts,
        "open": open_price,
        "close": close,
        "high": high,
        "low": low,
        "volume": _to_int(row.get("成交量") or row.get("volume")),
        "amount": _to_float(row.get("成交额") or row.get("amount")),
    }


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _parse_ts(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


class _MinRow(TypedDict):
    ts: datetime
    open: float
    close: float
    high: float
    low: float
    volume: int | None
    amount: float | None
