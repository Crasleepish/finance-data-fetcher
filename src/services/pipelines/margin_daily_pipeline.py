from __future__ import annotations

from dataclasses import dataclass, field

from core.calendar.service import TradingCalendarService
from core.chunking.policies import TradeDayRangeChunkPolicy
from core.clean.margin_daily_cleaner import MarginDailyCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_margin_daily_fetcher import TushareMarginDailyFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class MarginDailyPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning Tushare margin daily-summary data."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    _chunk_policy: TradeDayRangeChunkPolicy = field(init=False, repr=False)
    _fetcher: TushareMarginDailyFetcher = field(init=False, repr=False)
    _cleaner: MarginDailyCleaner = field(init=False, repr=False)

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
            TushareMarginDailyFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", MarginDailyCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan trade-day-aware date range chunks."""
        return self._chunk_policy.plan(arguments)

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw margin data for a date range."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw margin data into DB-ready records."""
        return self._cleaner.clean(raw_batch)
