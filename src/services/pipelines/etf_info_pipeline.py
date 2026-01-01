from __future__ import annotations

from dataclasses import dataclass, field

from core.clean.etf_info_cleaner import EtfInfoCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_fund_basic_fetcher import TushareFundBasicFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class EtfInfoPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning etf_info records."""

    client: TushareClient
    retry_policy: RetryPolicy
    _fetcher: TushareFundBasicFetcher = field(init=False, repr=False)
    _cleaner: EtfInfoCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareFundBasicFetcher(
                client=self.client,
                retry_policy=self.retry_policy,
                page_size=4000,
            ),
        )
        object.__setattr__(self, "_cleaner", EtfInfoCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Return a single chunk; pagination is handled by the fetcher."""
        params = dict(arguments.get("params", {}))
        params.setdefault("market", "E")
        params.setdefault("status", "L")
        return [{"params": params}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_basic data for the chunk."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw fund_basic data into etf_info records."""
        return self._cleaner.clean(raw_batch)
