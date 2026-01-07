from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from core.clean.rt_market_factors_cleaner import RtMarketFactorsCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.db.tables import rt_market_factors
from services.rt_market_factors_fetcher import RtMarketFactorsFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RtMarketFactorsPipeline(IngestionPipeline):
    """Pipeline for computing intraday market_factors snapshots."""

    engine: Engine
    rt_fetch_interval_s: int

    def plan_chunks(self, _: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk if interval allows fetching."""
        if not _should_fetch(self.engine, self.rt_fetch_interval_s):
            return []
        return [{"params": {}}]

    def fetch(self, _: ChunkArgs) -> RawBatch:
        """Compute intraday factor snapshot."""
        return RtMarketFactorsFetcher(engine=self.engine).fetch_snapshot()

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw snapshot rows into rt_market_factors records."""
        return RtMarketFactorsCleaner().clean(raw_batch)


def _should_fetch(engine: Engine, interval_s: int) -> bool:
    with engine.begin() as connection:
        latest = connection.execute(select(func.max(rt_market_factors.c.latest_date))).scalar()
    if latest is None:
        return True
    elapsed = (datetime.now() - latest).total_seconds()
    if elapsed < interval_s:
        logger.info(
            "rt market factors skipped due to fetch interval",
            extra={"elapsed_s": int(elapsed), "interval_s": interval_s},
        )
        return False
    return True
