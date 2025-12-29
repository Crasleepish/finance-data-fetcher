from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from core.calendar.service import TradingCalendarService
from core.clean.stock_hist_unadj_cleaner import StockHistUnadjCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_stock_hist_fetcher import TushareStockHistUnadjFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class StockHistUnadjPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning stock_hist_unadj data."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    _fetcher: TushareStockHistUnadjFetcher = field(init=False, repr=False)
    _cleaner: StockHistUnadjCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareStockHistUnadjFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", StockHistUnadjCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan one chunk per trade date."""
        params = dict(arguments.get("params", {}))
        start = _parse_date(_require_param(params, "start_date"))
        end = _parse_date(_require_param(params, "end_date"))
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=1)
        return [{"params": {"trade_date": chunk[0].isoformat()}} for chunk in chunks if chunk]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw daily data for a trade date."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw daily data into stock_hist_unadj records."""
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
