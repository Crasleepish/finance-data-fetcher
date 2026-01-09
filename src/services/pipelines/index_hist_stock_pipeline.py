from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sqlalchemy import Table
from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.index_hist_stock_cleaner import IndexHistStockCleaner
from core.fetch.retry import RetryPolicy
from core.indexing.index_codes import IndexCodeMapping, build_code_mappings, parse_index_codes
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_index_daily_fetcher import TushareIndexDailyFetcher
from infra.index_catalog import require_index_codes
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class IndexHistStockPipeline(IngestionPipeline):
    """Pipeline for fetching stock index history from Tushare index_daily."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    engine: Engine
    index_info_table: Table
    codes_raw: str
    _fetcher: TushareIndexDailyFetcher = field(init=False, repr=False)
    _cleaner: IndexHistStockCleaner = field(init=False, repr=False)
    _codes: list[IndexCodeMapping] = field(init=False, repr=False)
    _validated: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_codes", [])
        object.__setattr__(self, "_validated", False)
        object.__setattr__(
            self, "_fetcher", TushareIndexDailyFetcher(self.client, self.retry_policy)
        )
        object.__setattr__(self, "_cleaner", IndexHistStockCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan chunks by natural year."""
        self._ensure_initialized()
        if not self._codes:
            return []
        params = dict(arguments.get("params", {}))
        start = _parse_date(_require_param(params, "start_date"))
        end = _parse_date(_require_param(params, "end_date"))
        chunks = _plan_year_chunks(start, end)
        return [
            {
                "params": {
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "codes": self._codes,
                }
            }
            for chunk_start, chunk_end in chunks
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw stock index history for a trade date."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw stock index history into index_hist records."""
        return self._cleaner.clean(raw_batch)

    def _ensure_initialized(self) -> None:
        if self._validated:
            return
        codes = require_index_codes(
            self.engine,
            self.index_info_table,
            parse_index_codes(self.codes_raw),
        )
        object.__setattr__(self, "_codes", build_code_mappings(codes, _identity))
        object.__setattr__(self, "_validated", True)


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


def _identity(value: str) -> str:
    return value


def _plan_year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        year_end = date(current.year, 12, 31)
        chunk_end = min(end, year_end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks
