from __future__ import annotations

from dataclasses import dataclass, field

from core.clean.index_info_cleaner import IndexInfoCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_index_basic_fetcher import TushareIndexBasicFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class IndexInfoPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning index_info records."""

    client: TushareClient
    retry_policy: RetryPolicy
    _fetcher: TushareIndexBasicFetcher = field(init=False, repr=False)
    _cleaner: IndexInfoCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareIndexBasicFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", IndexInfoCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Return a single chunk; pagination is handled by the fetcher."""
        params = dict(arguments.get("params", {}))
        params.setdefault("markets", ["CSI", "SSE", "SZSE"])
        params.setdefault("csv_path", "extra/additional_index_info.csv")
        return [{"params": params}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index_basic data plus CSV overrides."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean and merge index_basic data into index_info records."""
        return self._cleaner.clean(raw_batch)
