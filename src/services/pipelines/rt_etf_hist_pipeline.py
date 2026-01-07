from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from core.clean.rt_etf_hist_cleaner import RtEtfHistCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.db.tables import etf_info, rt_etf_hist
from infra.etf_catalog import load_etf_codes
from infra.fetcher.akshare_etf_sina_fetcher import AkshareEtfSinaFetcher
from infra.fetcher.pysnowball_etf_quotec_fetcher import EtfCodeMapping, PysnowballEtfQuotecFetcher
from infra.xueqiu_token_fetcher import XueqiuTokenFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RtEtfHistAksharePipeline(IngestionPipeline):
    """Real-time ETF snapshot pipeline via Akshare."""

    engine: Engine
    rt_fetch_interval_s: int
    fetcher: AkshareEtfSinaFetcher
    _cleaner: RtEtfHistCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_cleaner", RtEtfHistCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [{"params": {}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw ETF snapshot rows."""
        rows = self.fetcher.fetch(chunk_args)
        if not rows:
            raise RuntimeError("akshare etf returned empty snapshot data")
        latest_time = datetime.now()
        normalized = _normalize_akshare_rows(rows, latest_time)
        if not normalized:
            raise RuntimeError("akshare etf returned empty snapshot data")
        return normalized

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_etf_hist records."""
        return self._cleaner.clean(raw_batch)


@dataclass(frozen=True)
class RtEtfHistXueqiuPipeline(IngestionPipeline):
    """Real-time ETF snapshot pipeline via pysnowball."""

    engine: Engine
    rt_fetch_interval_s: int
    _fetcher: PysnowballEtfQuotecFetcher = field(init=False, repr=False)
    _cleaner: RtEtfHistCleaner = field(init=False, repr=False)
    _codes: list[EtfCodeMapping] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        codes = load_etf_codes(self.engine, etf_info, "%ETF%")
        mappings = [_to_xueqiu_mapping(code) for code in codes]
        object.__setattr__(self, "_codes", mappings)
        object.__setattr__(
            self,
            "_fetcher",
            PysnowballEtfQuotecFetcher(token_fetcher=XueqiuTokenFetcher()),
        )
        object.__setattr__(self, "_cleaner", RtEtfHistCleaner())

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan chunks of up to 100 ETF codes."""
        if not self._codes:
            return []
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        self._fetcher.reset_refresh_state()
        chunks = _split_chunks(self._codes, 100)
        return [{"params": {"codes": chunk}} for chunk in chunks]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw ETF snapshot rows for a chunk."""
        rows = self._fetcher.fetch(chunk_args)
        if not rows:
            raise RuntimeError("pysnowball etf returned empty snapshot data")
        latest_time = datetime.now()
        return [{**row, "latest_time": latest_time} for row in rows]

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_etf_hist records."""
        return self._cleaner.clean(raw_batch)


def _should_fetch(engine: Engine, interval_s: int) -> bool:
    with engine.begin() as connection:
        latest = connection.execute(select(func.max(rt_etf_hist.c.latest_time))).scalar()
    if latest is None:
        return True
    elapsed = (datetime.now() - latest).total_seconds()
    if elapsed < interval_s:
        logger.info(
            "rt etf snapshot skipped due to fetch interval",
            extra={"elapsed_s": int(elapsed), "interval_s": interval_s},
        )
        return False
    return True


def _to_xueqiu_mapping(etf_code: str) -> EtfCodeMapping:
    if "." not in etf_code:
        return {"etf_code": etf_code, "api_code": etf_code}
    base, suffix = etf_code.split(".", 1)
    return {"etf_code": etf_code, "api_code": f"{suffix}{base}"}


def _split_chunks(values: list[EtfCodeMapping], size: int) -> list[list[EtfCodeMapping]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _normalize_akshare_rows(rows: RawBatch, latest_time: datetime) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code_raw = row.get("代码")
        if not isinstance(code_raw, str) or not code_raw:
            continue
        etf_code = _normalize_etf_code(code_raw)
        normalized.append(
            {
                "etf_code": etf_code,
                "open": row.get("今开"),
                "close": row.get("最新价"),
                "high": row.get("最高"),
                "low": row.get("最低"),
                "pre_close": row.get("昨收"),
                "volume": row.get("成交量"),
                "amount": row.get("成交额"),
                "latest_time": latest_time,
            }
        )
    logger.info(
        "akshare etf normalized",
        extra={"row_count": len(normalized)},
    )
    return normalized


def _normalize_etf_code(code: str) -> str:
    code = code.strip().upper()
    if "." in code:
        return code
    if code.startswith("SH"):
        return f"{code[2:]}.SH"
    if code.startswith("SZ"):
        return f"{code[2:]}.SZ"
    return code
