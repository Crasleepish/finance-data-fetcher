from __future__ import annotations

from typing import Protocol

from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch


class IngestionPipeline(Protocol):
    """Core pipeline interface for chunking, fetching, and cleaning data."""

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan chunk arguments for a pipeline run."""

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for a chunk (no state transitions here)."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize raw data into the pipeline's output schema."""
