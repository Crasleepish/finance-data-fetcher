from __future__ import annotations

import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from core.clean.stock_is_st_from_file_cleaner import StockIsStFromFileCleaner
from core.fetch.errors import NonRetryableError
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.stock_is_st_from_file_fetcher import StockIsStFromFileFetcher

_MEMBER_GROUP_SIZE = 200


@dataclass(frozen=True)
class StockIsStFromFilePipeline(IngestionPipeline):
    """Pipeline for loading stock is_st history from a zip file."""

    _fetcher: StockIsStFromFileFetcher = field(default_factory=StockIsStFromFileFetcher, repr=False)
    _cleaner: StockIsStFromFileCleaner = field(default_factory=StockIsStFromFileCleaner, repr=False)

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        params = dict(arguments.get("params", {}))
        zip_path = params.get("zip_path")
        if not isinstance(zip_path, str) or not zip_path:
            raise NonRetryableError("zip_path must be a string")

        path = Path(zip_path)
        if not path.exists():
            raise NonRetryableError(f"zip file not found: {path}")

        with zipfile.ZipFile(path) as archive:
            member_names = sorted(
                name
                for name in archive.namelist()
                if name.endswith(".csv") and not name.endswith("/")
            )
        if not member_names:
            raise NonRetryableError("zip archive contains no csv members")

        return [
            {
                "params": {
                    "zip_path": str(path),
                    "member_names": member_names[start : start + _MEMBER_GROUP_SIZE],
                }
            }
            for start in range(0, len(member_names), _MEMBER_GROUP_SIZE)
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        return self._cleaner.clean(raw_batch)
