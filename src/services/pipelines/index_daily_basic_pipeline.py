from __future__ import annotations

from dataclasses import dataclass, field

from core.calendar.service import TradingCalendarService
from core.chunking.policies import TradeDayRangePerCodeChunkPolicy
from core.clean.index_daily_basic_cleaner import IndexDailyBasicCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_index_daily_basic_fetcher import TushareIndexDailyBasicFetcher
from infra.tushare.client import TushareClient

# The six indices documented for Tushare `index_dailybasic` (doc_id=128).
# Source-native ts_code values, stored as-is (no relabelling).
INDEX_DAILY_BASIC_SOURCE_CODES = (
    "000001.SH",
    "399001.SZ",
    "000016.SH",
    "000905.SH",
    "399005.SZ",
    "399006.SZ",
)


@dataclass(frozen=True)
class IndexDailyBasicPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning Tushare 大盘指数每日指标 data."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    _chunk_policy: TradeDayRangePerCodeChunkPolicy = field(init=False, repr=False)
    _fetcher: TushareIndexDailyBasicFetcher = field(init=False, repr=False)
    _cleaner: IndexDailyBasicCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_chunk_policy",
            TradeDayRangePerCodeChunkPolicy(
                start_key="start_date",
                end_key="end_date",
                code_key="ts_code",
                chunk_size=300,
                codes=INDEX_DAILY_BASIC_SOURCE_CODES,
                calendar=self.calendar,
            ),
        )
        object.__setattr__(
            self,
            "_fetcher",
            TushareIndexDailyBasicFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", IndexDailyBasicCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan trade-day-aware date range chunks for every source code."""
        return self._chunk_policy.plan(arguments)

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index_dailybasic data for a chunk."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw index_dailybasic data into DB-ready records."""
        return self._cleaner.clean(raw_batch)
