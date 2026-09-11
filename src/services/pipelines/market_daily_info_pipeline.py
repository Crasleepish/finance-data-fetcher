from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from core.calendar.service import TradingCalendarService
from core.chunking.policies import TradeDayRangeChunkPolicy
from core.clean.market_daily_info_cleaner import MarketDailyInfoCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_market_daily_info_fetcher import TushareMarketDailyInfoFetcher
from infra.fetcher.tushare_sz_daily_info_fetcher import TushareSzDailyInfoFetcher
from infra.tushare.client import TushareClient

# sz_daily_info coverage starts 2008-01-02; no SZ chunks are planned before it.
SZ_SOURCE_START_DATE = date(2008, 1, 2)


@dataclass(frozen=True)
class MarketDailyInfoPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning Tushare 市场交易统计 data.

    Dual source: SH rows come exclusively from pro.daily_info (requested with
    exchange='SH'), SZ rows exclusively from pro.sz_daily_info. Each source is
    planned independently into 50-trade-day chunks; SH chunks are planned
    first, then SZ chunks (clamped to 2008-01-02 onward). Chunks are tagged
    with a `source` discriminator ("sh"/"sz") used to route fetches; both
    sources share the same cleaner, which applies source-specific
    normalization based on per-row `_source` tags.
    """

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    _chunk_policy: TradeDayRangeChunkPolicy = field(init=False, repr=False)
    _fetcher: TushareMarketDailyInfoFetcher = field(init=False, repr=False)
    _sz_fetcher: TushareSzDailyInfoFetcher = field(init=False, repr=False)
    _cleaner: MarketDailyInfoCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        # Conservative 50-trade-day chunks per source: pro.daily_info (SH
        # exchange filter) caps responses at 4000 rows, and pro.sz_daily_info
        # returns ~16 category rows per trade day with a 2000-row cap, so both
        # stay well below their per-request row limits.
        object.__setattr__(
            self,
            "_chunk_policy",
            TradeDayRangeChunkPolicy(
                start_key="start_date",
                end_key="end_date",
                chunk_size=50,
                calendar=self.calendar,
            ),
        )
        object.__setattr__(
            self,
            "_fetcher",
            TushareMarketDailyInfoFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(
            self,
            "_sz_fetcher",
            TushareSzDailyInfoFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", MarketDailyInfoCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan SH chunks first, then SZ chunks (>= 2008-01-02), 50 trade days each."""
        params = arguments.get("params") or {}
        for key in ("start_date", "end_date"):
            if key not in params:
                raise KeyError(f"missing required param: {key}")
        start_date = _parse_date_param(params.get("start_date"))
        end_date = _parse_date_param(params.get("end_date"))
        # The shared TradeDayRangeChunkPolicy only accepts YYYY-MM-DD strings
        # (or date objects), while the documented input contract is YYYY-MM-DD
        # or YYYYMMDD; normalize both dates to ISO YYYY-MM-DD before planning
        # so SH and SZ chunks are planned from the same normalized range.
        normalized: Arguments = {
            "params": {
                **params,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
        }
        sh_chunks = [
            self._with_source(chunk, "sh") for chunk in self._chunk_policy.plan(normalized)
        ]

        sz_start = max(start_date, SZ_SOURCE_START_DATE)
        if sz_start > end_date:
            return sh_chunks
        sz_arguments: Arguments = {
            "params": {**normalized["params"], "start_date": sz_start.isoformat()},
        }
        sz_chunks = [
            self._with_source(chunk, "sz") for chunk in self._chunk_policy.plan(sz_arguments)
        ]
        return sh_chunks + sz_chunks

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Route a chunk to the fetcher matching its source tag."""
        source = (chunk_args.get("params") or {}).get("source")
        if source == "sz":
            return self._sz_fetcher.fetch(chunk_args)
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw daily-info rows (SH or SZ) into DB-ready records."""
        return self._cleaner.clean(raw_batch)

    @staticmethod
    def _with_source(chunk: ChunkArgs, source: str) -> ChunkArgs:
        params = dict(chunk.get("params") or {})
        params["source"] = source
        return {"params": params}


def _parse_date_param(value: object) -> date:
    """Parse a date-range param: date objects and YYYY-MM-DD or YYYYMMDD strings."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("expected date or YYYY-MM-DD or YYYYMMDD string")
