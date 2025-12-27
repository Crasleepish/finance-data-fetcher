from __future__ import annotations

from typing import Protocol

from core.pipeline.types import NormalizedBatch, RawBatch


class Cleaner(Protocol):
    """Cleaner interface for normalizing raw batches."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw data into DB-ready records."""
