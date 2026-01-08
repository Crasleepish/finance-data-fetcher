from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Engine

from core.clean.market_factors_cleaner import MarketFactorsCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from services.factor_fetcher import FactorFetcher


@dataclass(frozen=True)
class MarketFactorsPipeline(IngestionPipeline):
    """Pipeline for computing and persisting market_factors."""

    engine: Engine

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk for the requested date range."""
        params = dict(arguments.get("params", {}))
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        mode = params.get("mode", "history")
        if not isinstance(mode, str):
            raise ValueError("mode must be a string")
        return [{"params": {"start_date": start_date, "end_date": end_date, "mode": mode}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Compute factor raw batch for the requested date range."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        mode = params.get("mode", "history")
        if not isinstance(mode, str):
            raise ValueError("mode must be a string")
        cancel_check = chunk_args.get("cancel_check")
        return FactorFetcher(engine=self.engine).fetch_all(
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            cancel_check=cancel_check if callable(cancel_check) else None,
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Return normalized market_factors records."""
        return MarketFactorsCleaner().clean(raw_batch)


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value
