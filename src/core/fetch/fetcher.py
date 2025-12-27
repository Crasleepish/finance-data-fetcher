from __future__ import annotations

from typing import Protocol

from core.pipeline.types import ChunkArgs, RawBatch


class Fetcher(Protocol):
    """Fetcher interface for retrieving raw data for a chunk."""

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for the given chunk arguments."""
