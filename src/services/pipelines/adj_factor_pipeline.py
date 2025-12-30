from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from core.calendar.service import TradingCalendarService
from core.clean.adj_factor_cleaner import AdjFactorCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_adj_factor_fetcher import TushareAdjFactorFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class AdjFactorPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning adj_factor data."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    _fetcher: TushareAdjFactorFetcher = field(init=False, repr=False)
    _cleaner: AdjFactorCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareAdjFactorFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", AdjFactorCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan one chunk per trade date."""
        params = dict(arguments.get("params", {}))
        start = _parse_date(_require_param(params, "start_date"))
        end = _parse_date(_require_param(params, "end_date"))
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=1)
        return [{"params": {"trade_date": chunk[0].isoformat()}} for chunk in chunks if chunk]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw adj_factor data for a trade date."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw adj_factor data into adj_factor records."""
        return self._cleaner.clean(raw_batch)


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _parse_date(value: str) -> date:
    if len(value) == 10:
        return datetime.strptime(value, "%Y-%m-%d").date()
    if len(value) == 8 and value.isdigit():
        return datetime.strptime(value, "%Y%m%d").date()
    raise ValueError("expected YYYY-MM-DD or YYYYMMDD date string")
