from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy.engine import Engine

from config.settings import GoldDataConfig
from core.clean.gold_future_curve_cleaner import GoldFutureCurveCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.gold_derivatives_fetcher import GoldDerivativesFetcher


@dataclass(frozen=True)
class GoldFutureCurvePipeline(IngestionPipeline):
    """Pipeline for fetching and persisting gold futures curve data."""

    engine: Engine
    config: GoldDataConfig
    _fetcher: GoldDerivativesFetcher = field(init=False, repr=False)
    _cleaner: GoldFutureCurveCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            GoldDerivativesFetcher(engine=self.engine, config=self.config),
        )
        object.__setattr__(self, "_cleaner", GoldFutureCurveCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk for the futures curve snapshot."""
        _ = arguments
        return [{}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch gold futures curve records."""
        cancel_check = chunk_args.get("cancel_check")
        return self._fetcher.update_barchart_future_curve(
            cancel_check=cancel_check if callable(cancel_check) else None
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize futures curve records for persistence."""
        return self._cleaner.clean(raw_batch)
