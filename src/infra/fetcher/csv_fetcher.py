from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from core.fetch.errors import NonRetryableError
from core.fetch.fetcher import Fetcher
from core.pipeline.types import ChunkArgs, RawBatch


@dataclass(frozen=True)
class CsvFetcher(Fetcher):
    """Fetch raw data from a CSV file path provided in chunk args."""

    path_key: str = "csv_path"

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        params = chunk_args.get("params")
        if not isinstance(params, dict):
            raise NonRetryableError("params must be a mapping")

        path_value = params.get(self.path_key)
        if not isinstance(path_value, str):
            raise NonRetryableError("csv_path must be a string")

        path = Path(path_value)
        if not path.exists():
            raise NonRetryableError(f"csv file not found: {path}")

        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]
