from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Table, func, select
from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.etf_daily_size_cleaner import (
    SEED_SOURCE,
    EtfDailySizeCleaner,
    optional_decimal,
)
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_etf_daily_size_fetcher import TushareEtfDailySizeFetcher
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EtfDailySizePipeline(IngestionPipeline):
    """Pipeline for ETF daily size: one trade day per chunk (fund_share x fund_nav).

    Each chunk fetches fund_share for SH and SZ plus fund_nav for market E on
    the same date. The ETF universe is authoritative from fund_share only:

    - codes with a valid same-day fund_share event (this bootstraps the initial
      universe when syncing from the earliest available date on an empty table);
    - every code already present in the target table (`trade_date < D`, latest
      row per code), whose share is forward-filled.

    That universe is left-joined to the day's fund_nav rows. The fund_nav
    market E feed also returns non-ETF funds, so NAV-only codes never widen the
    universe, never produce records, and never trigger historical fund_share
    lookups. Codes without any NAV row on D still produce a record with
    `nav_missing=True` and NULL NAV/AUM columns. Codes whose same-day share is
    non-finite fall back to their stored seed, and are skipped when no seed
    exists. NAV is never forward-filled; shares are never defaulted to 0; no
    value is ever sourced from dates after D.
    """

    calendar: TradingCalendarService
    client: TushareClient
    retry_policy: RetryPolicy
    engine: Engine
    target_table: Table
    _fetcher: TushareEtfDailySizeFetcher = field(init=False, repr=False)
    _cleaner: EtfDailySizeCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareEtfDailySizeFetcher(self.client, self.retry_policy),
        )
        object.__setattr__(self, "_cleaner", EtfDailySizeCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan one chunk per trade day in ascending order."""
        params = dict(arguments.get("params", {}))
        start = _require_date_param(params, "start_date")
        end = _require_date_param(params, "end_date")
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=1)
        return [{"params": {"trade_date": chunk[0].isoformat()}} for chunk in chunks if chunk]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch one trade day plus the stored seeds for every known ETF."""
        params = chunk_args.get("params") or {}
        trade_date = _require_date_param(params, "trade_date")
        rows: list[dict[str, object]] = [
            {**row, "_chunk_date": trade_date} for row in self._fetcher.fetch(chunk_args)
        ]
        db_seeds = self._load_db_seeds(trade_date)
        rows.extend(
            {
                "_source": SEED_SOURCE,
                "_chunk_date": trade_date,
                "ts_code": ts_code,
                "trade_date": source_date,
                "fd_share": fd_share,
            }
            for ts_code, (source_date, fd_share) in sorted(db_seeds.items())
        )
        return rows

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw fund_share/fund_nav records into etf_daily_size rows."""
        return self._cleaner.clean(raw_batch)

    def _load_db_seeds(self, trade_date: date) -> dict[str, tuple[date, Decimal]]:
        """Load the latest `trade_date < D` fd_share for every code in the table.

        Uses `row_number() over (partition by ts_code order by trade_date desc)`
        filtered to rn = 1 instead of PostgreSQL-only DISTINCT ON.
        """
        table = self.target_table
        ranked = (
            select(
                table.c.ts_code,
                table.c.trade_date,
                table.c.share_source_date,
                table.c.fd_share,
                func.row_number()
                .over(
                    partition_by=table.c.ts_code,
                    order_by=table.c.trade_date.desc(),
                )
                .label("rn"),
            )
            .where(table.c.trade_date < trade_date)
            .where(table.c.fd_share.is_not(None))
            .subquery()
        )
        stmt = select(
            ranked.c.ts_code,
            ranked.c.trade_date,
            ranked.c.share_source_date,
            ranked.c.fd_share,
        ).where(ranked.c.rn == 1)
        seeds: dict[str, tuple[date, Decimal]] = {}
        with self.engine.begin() as connection:
            for row in connection.execute(stmt).mappings():
                source_date = row["share_source_date"] or row["trade_date"]
                fd_share = optional_decimal(row["fd_share"])
                if fd_share is None or source_date > trade_date:
                    continue
                seeds[str(row["ts_code"])] = (source_date, fd_share)
        return seeds


def _require_date_param(params: dict[str, object], key: str) -> date:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    parsed = _parse_date(value)
    if parsed is None:
        raise ValueError(f"invalid param: {key}")
    return parsed


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    try:
        if value != value:  # NaN/NaT-like missing values
            return None
    except Exception:
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"invalid date: {value!r}")
