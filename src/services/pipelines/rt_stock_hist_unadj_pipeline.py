from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from core.clean.rt_stock_hist_unadj_cleaner import RtStockHistUnadjCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.db.tables import rt_stock_hist_unadj
from infra.fetcher.akshare_stock_spot_fetcher import AkshareStockSpotFetcher
from infra.fetcher.tushare_rt_k_fetcher import TushareRtKFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RtStockHistUnadjTusharePipeline(IngestionPipeline):
    """Real-time stock snapshot pipeline using Tushare rt_k."""

    engine: Engine
    fetcher: TushareRtKFetcher
    rt_fetch_interval_s: int
    _cleaner: RtStockHistUnadjCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_cleaner", RtStockHistUnadjCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [{"params": {}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw rt_k rows and normalize to snapshot format."""
        rows = self.fetcher.fetch(chunk_args)
        if not rows:
            return []
        latest_time = datetime.now()
        return _normalize_tushare_rows(rows, latest_time)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows for persistence."""
        return self._cleaner.clean(raw_batch)


@dataclass(frozen=True)
class RtStockHistUnadjAksharePipeline(IngestionPipeline):
    """Real-time stock snapshot pipeline using Akshare spot data."""

    engine: Engine
    fetcher: AkshareStockSpotFetcher
    rt_fetch_interval_s: int
    _cleaner: RtStockHistUnadjCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_cleaner", RtStockHistUnadjCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [{"params": {}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw spot rows and normalize to snapshot format."""
        rows = self.fetcher.fetch(chunk_args)
        if not rows:
            return []
        latest_time = datetime.now()
        return _normalize_akshare_rows(rows, latest_time)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows for persistence."""
        return self._cleaner.clean(raw_batch)


def _should_fetch(engine: Engine, interval_s: int) -> bool:
    with engine.begin() as connection:
        latest = connection.execute(select(func.max(rt_stock_hist_unadj.c.latest_time))).scalar()
    if latest is None:
        return True
    elapsed = (datetime.now() - latest).total_seconds()
    if elapsed < interval_s:
        logger.info(
            "rt snapshot skipped due to fetch interval",
            extra={"elapsed_s": int(elapsed), "interval_s": interval_s},
        )
        return False
    return True


def _normalize_tushare_rows(rows: RawBatch, latest_time: datetime) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts_code = row.get("ts_code")
        if not isinstance(ts_code, str) or not ts_code:
            continue
        normalized.append(
            {
                "stock_code": ts_code,
                "open": _to_float(row.get("open")),
                "close": _to_float(row.get("close")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "pre_close": _to_float(row.get("pre_close")),
                "volume": _to_int(row.get("vol")),
                "amount": _to_float(row.get("amount")),
                "latest_time": latest_time,
            }
        )
    logger.info(
        "rt_k normalized",
        extra={"row_count": len(normalized)},
    )
    return normalized


def _normalize_akshare_rows(rows: RawBatch, latest_time: datetime) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code_raw = _first_str(row.get("代码"), row.get("code"), row.get("ts_code"))
        if not code_raw:
            continue
        stock_code = _normalize_stock_code(code_raw)
        volume_raw = row.get("成交量") or row.get("volume") or row.get("vol")
        amount_raw = row.get("成交额") or row.get("amount")
        normalized.append(
            {
                "stock_code": stock_code,
                "open": _to_float(row.get("今开") or row.get("open")),
                "close": _to_float(row.get("最新价") or row.get("close") or row.get("现价")),
                "high": _to_float(row.get("最高") or row.get("high")),
                "low": _to_float(row.get("最低") or row.get("low")),
                "pre_close": _to_float(row.get("昨收") or row.get("pre_close")),
                "volume": _to_int(_to_shares(volume_raw)),
                "amount": _to_float(amount_raw),
                "latest_time": latest_time,
            }
        )
    logger.info(
        "akshare spot normalized",
        extra={"row_count": len(normalized)},
    )
    return normalized


def _first_str(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _normalize_stock_code(code: str) -> str:
    code = code.strip().upper()
    if "." in code:
        return code
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    if code.startswith(("4", "8", "9")):
        return f"{code}.BJ"
    return code


def _to_shares(value: Any) -> Any:
    if value is None:
        return None
    try:
        return float(value) * 100.0
    except (TypeError, ValueError):
        return value


def _to_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip() == "":
        return None
    return float(value)


def _to_int(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip() == "":
        return None
    return int(float(value))
