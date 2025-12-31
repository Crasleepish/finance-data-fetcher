from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy import Table
from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.index_hist_gold_cleaner import IndexHistGoldCleaner
from core.fetch.retry import RetryPolicy
from core.indexing.index_codes import (
    IndexCodeMapping,
    build_code_mappings,
    parse_index_codes,
    strip_sge_suffix,
)
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_sge_daily_fetcher import TushareSgeDailyFetcher
from infra.index_catalog import require_index_codes
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class IndexHistGoldPipeline(IngestionPipeline):
    """Pipeline for fetching gold index history from Tushare sge_daily."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    engine: Engine
    index_info_table: Table
    codes_raw: str
    _fetcher: TushareSgeDailyFetcher = field(init=False, repr=False)
    _cleaner: IndexHistGoldCleaner = field(init=False, repr=False)
    _codes: list[IndexCodeMapping] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        codes = require_index_codes(
            self.engine,
            self.index_info_table,
            parse_index_codes(self.codes_raw),
        )
        object.__setattr__(self, "_codes", build_code_mappings(codes, strip_sge_suffix))
        object.__setattr__(self, "_fetcher", TushareSgeDailyFetcher(self.client, self.retry_policy))
        object.__setattr__(self, "_cleaner", IndexHistGoldCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan one chunk per trade date."""
        if not self._codes:
            return []
        params = dict(arguments.get("params", {}))
        start = _parse_date(_require_param(params, "start_date"))
        end = _parse_date(_require_param(params, "end_date"))
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=1)
        return [
            {"params": {"trade_date": chunk[0].isoformat(), "codes": self._codes}}
            for chunk in chunks
            if chunk
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw gold index history for a trade date."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw gold index history into index_hist records."""
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
