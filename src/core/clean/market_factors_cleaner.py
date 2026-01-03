from __future__ import annotations

from core.pipeline.types import NormalizedBatch, RawBatch


class MarketFactorsCleaner:
    """Pass-through cleaner for market_factors records."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Return raw batch as normalized records without transformation."""
        return [dict(item) for item in raw_batch]
