from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from core.clean.rt_index_hist_cleaner import RtIndexHistCleaner
from core.indexing.index_codes import IndexCodeMapping, build_code_mappings, parse_index_codes
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.db.tables import index_info, rt_index_hist
from infra.fetcher.pysnowball_quotec_fetcher import PysnowballQuotecFetcher
from infra.fetcher.tushare_rt_idx_k_fetcher import TushareRtIdxKFetcher
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
class RtIndexHistTusharePipeline(IngestionPipeline):
    """Real-time index snapshot pipeline via Tushare rt_idx_k."""

    engine: Engine
    rt_fetch_interval_s: int
    fetcher: TushareRtIdxKFetcher
    codes_raw: str
    _cleaner: RtIndexHistCleaner = field(init=False, repr=False)
    _codes: list[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        codes = require_index_codes(self.engine, index_info, parse_index_codes(self.codes_raw))
        object.__setattr__(self, "_codes", codes)
        object.__setattr__(self, "_cleaner", RtIndexHistCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not self._codes:
            return []
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [{"params": {"codes": self._codes}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index snapshot rows."""
        rows = self.fetcher.fetch(chunk_args)
        if not rows:
            raise RuntimeError("tushare rt_idx_k returned empty snapshot data")
        latest_time = datetime.now()
        normalized: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = row.get("ts_code")
            if not isinstance(code, str) or not code:
                continue
            normalized.append(
                {
                    "index_code": code,
                    "open": row.get("open"),
                    "close": row.get("close"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "pre_close": row.get("pre_close"),
                    "volume": row.get("vol"),
                    "amount": row.get("amount"),
                    "latest_time": latest_time,
                }
            )
        if not normalized:
            raise RuntimeError("tushare rt_idx_k returned empty snapshot data")
        return normalized

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
