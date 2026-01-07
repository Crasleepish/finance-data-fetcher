from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy import Table
from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.fund_hist_cleaner import FundHistCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_fund_nav_fetcher import TushareFundNavFetcher
from infra.fund_catalog import parse_fund_codes, validate_money_fund_codes
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class FundHistMoneyPipeline(IngestionPipeline):
    """Pipeline for fetching configured money fund NAV history."""

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    engine: Engine
    fund_info_table: Table
    codes_raw: str
    _fetcher: TushareFundNavFetcher = field(init=False, repr=False)
    _cleaner: FundHistCleaner = field(init=False, repr=False)
    _codes: list[str] = field(init=False, repr=False)
    _validated: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        codes = parse_fund_codes(self.codes_raw)
        object.__setattr__(self, "_codes", codes)
        object.__setattr__(self, "_validated", not codes)
        object.__setattr__(self, "_fetcher", TushareFundNavFetcher(self.client, self.retry_policy))
        object.__setattr__(self, "_cleaner", FundHistCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan one chunk per trade date."""
        self._ensure_validated()
        if not self._codes:
            return []
        params = dict(arguments.get("params", {}))
        start = _parse_date(_require_param(params, "start_date"))
        end = _parse_date(_require_param(params, "end_date"))
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=1)
        return [
            {"params": {"nav_date": chunk[0].isoformat(), "codes": self._codes}}
            for chunk in chunks
            if chunk
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_nav data for a trade date."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw fund_nav data into fund_hist records."""
        return self._cleaner.clean(raw_batch)

    def _ensure_validated(self) -> None:
        if self._validated:
            return
        codes = validate_money_fund_codes(self.engine, self.fund_info_table, self._codes)
        object.__setattr__(self, "_codes", codes)
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
