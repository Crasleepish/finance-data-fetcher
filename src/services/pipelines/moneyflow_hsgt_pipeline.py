from __future__ import annotations

from dataclasses import dataclass, field

from core.calendar.service import TradingCalendarService
from core.chunking.policies import TradeDayRangeChunkPolicy
from core.clean.moneyflow_hsgt_cleaner import MoneyflowHsgtCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_moneyflow_hsgt_fetcher import TushareMoneyflowHsgtFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class MoneyflowHsgtPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning moneyflow_hsgt data."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    _chunk_policy: TradeDayRangeChunkPolicy = field(init=False, repr=False)
    _fetcher: TushareMoneyflowHsgtFetcher = field(init=False, repr=False)
    _cleaner: MoneyflowHsgtCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_chunk_policy",
            TradeDayRangeChunkPolicy(
                start_key="start_date",
                end_key="end_date",
                chunk_size=300,
                calendar=self.calendar,
            ),
        )
        object.__setattr__(
            self,
            "_fetcher",
            TushareMoneyflowHsgtFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", MoneyflowHsgtCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan trade-day-aware date range chunks."""
        return self._chunk_policy.plan(arguments)

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw moneyflow_hsgt data for a date range."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw moneyflow_hsgt data into DB-ready records."""
        return self._cleaner.clean(raw_batch)
