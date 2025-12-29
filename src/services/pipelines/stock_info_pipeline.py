from __future__ import annotations

from dataclasses import dataclass, field

from core.clean.stock_info_cleaner import StockInfoCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_stock_basic_fetcher import TushareStockBasicFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class StockInfoPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning stock_info records."""

    client: TushareClient
    retry_policy: RetryPolicy
    _fetcher: TushareStockBasicFetcher = field(init=False, repr=False)
    _cleaner: StockInfoCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareStockBasicFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", StockInfoCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Return a single chunk; pagination is handled by the fetcher."""
        params = dict(arguments.get("params", {}))
        params.setdefault("exchange", "")
        params.setdefault("list_statuses", ["L", "D", "P"])
        return [{"params": params}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw stock_basic data for the chunk."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw stock_basic data into stock_info records."""
        return self._cleaner.clean(raw_batch)
