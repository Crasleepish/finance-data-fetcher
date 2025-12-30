from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from math import isnan
from typing import Any

from sqlalchemy import Engine, Table, select

from core.clean.fundamental_data_cleaner import FundamentalDataCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_fundamental_fetcher import TushareFundamentalFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class FundamentalDataPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning quarterly fundamental data."""

    client: TushareClient
    retry_policy: RetryPolicy
    engine: Engine
    table: Table
    _fetcher: TushareFundamentalFetcher = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareFundamentalFetcher(self.client, self.retry_policy),
        )

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk for the requested quarterly range."""
        params = dict(arguments.get("params", {}))
        start_period = _require_param(params, "start_period")
        end_period = _require_param(params, "end_period")
        overwrite = bool(params.get("overwrite", False))
        return [
            {
                "params": {
                    "start_period": start_period,
                    "end_period": end_period,
                    "overwrite": overwrite,
                }
            }
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fundamental data for the quarterly range."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw data and compute operating_profit_ttm with history-aware filling."""
        overwrite = False
        if raw_batch:
            overwrite = bool(raw_batch[0].get("overwrite", False))
        cleaner = FundamentalDataCleaner(overwrite=overwrite)
        records = [dict(record) for record in cleaner.clean(raw_batch)]
        if not records:
            return []
        # For TTM: compute with historical operating_profit and forward fill within current batch.
        return _apply_operating_profit_ttm(
            records=records,
            engine=self.engine,
            table=self.table,
            overwrite=overwrite,
        )


def _apply_operating_profit_ttm(
    records: list[dict[str, Any]],
    engine: Engine,
    table: Table,
    overwrite: bool,
) -> list[dict[str, Any]]:
    # Load historical operating_profit and operating_profit_ttm to compute and fill TTM values.
    stock_codes = {record["stock_code"] for record in records if record.get("stock_code")}
    if not stock_codes:
        return records

    report_dates = sorted(
        record["report_date"] for record in records if isinstance(record.get("report_date"), date)
    )
    if not report_dates:
        return records

    min_date = report_dates[0]
    max_date = report_dates[-1]
    stmt = (
        select(
            table.c.stock_code,
            table.c.report_date,
            table.c.operating_profit,
            table.c.operating_profit_ttm,
        )
        .where(table.c.stock_code.in_(stock_codes))
        .where(table.c.report_date <= max_date)
    )
    history = {}
    with engine.begin() as connection:
        for row in connection.execute(stmt).mappings():
            history[(row["stock_code"], row["report_date"])] = {
                "operating_profit": row["operating_profit"],
                "operating_profit_ttm": row["operating_profit_ttm"],
            }

    # Build lookups for operating_profit to compute TTM across history+current batch.
    # Build operating_profit lookup across history and current batch.
    op_lookup: dict[tuple[str, date], float | None] = {
        (key[0], key[1]): value.get("operating_profit") for key, value in history.items()
    }
    for record in records:
        stock_code = record.get("stock_code")
        report_date = record.get("report_date")
        if isinstance(stock_code, str) and isinstance(report_date, date):
            op_lookup[(stock_code, report_date)] = record.get("operating_profit")

    # Compute TTM per stock_code/report_date and forward fill within the current batch only.
    # Forward fill uses latest historical TTM as seed but never mutates history rows.
    records_by_stock = _group_by_stock(records)
    for stock_code, items in records_by_stock.items():
        items.sort(key=lambda item: item["report_date"])

        # Seed forward fill from latest historical ttm before current batch.
        last_ttm = _latest_ttm_before(history, stock_code, min_date)
        for record in items:
            report_date = record["report_date"]
            existing_ttm = history.get((stock_code, report_date), {}).get("operating_profit_ttm")
            if not overwrite and existing_ttm is not None:
                record["operating_profit_ttm"] = existing_ttm
            else:
                # Calculate TTM using current quarter + last annual - last same period.
                op_t = op_lookup.get((stock_code, report_date))
                last_annual = op_lookup.get((stock_code, _last_annual_date(report_date)))
                last_same = op_lookup.get((stock_code, _last_same_period(report_date)))
                if _is_valid_number(op_t) and _is_valid_number(last_annual) and _is_valid_number(
                    last_same
                ):
                    record["operating_profit_ttm"] = op_t + last_annual - last_same
                else:
                    record["operating_profit_ttm"] = None

            # Forward fill only for current batch records (do not backfill history).
            if record.get("operating_profit_ttm") is None and last_ttm is not None:
                record["operating_profit_ttm"] = last_ttm
            else:
                last_ttm = record.get("operating_profit_ttm") or last_ttm
    return records


def _group_by_stock(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        stock_code = record.get("stock_code")
        report_date = record.get("report_date")
        if isinstance(stock_code, str) and isinstance(report_date, date):
            grouped.setdefault(stock_code, []).append(record)
    return grouped


def _latest_ttm_before(
    history: dict[tuple[str, date], dict[str, Any]], stock_code: str, cutoff: date
) -> float | None:
    candidates = [
        (key[1], value.get("operating_profit_ttm"))
        for key, value in history.items()
        if key[0] == stock_code
        and key[1] < cutoff
        and value.get("operating_profit_ttm") is not None
    ]
    if not candidates:
        return None
    latest = sorted(candidates, key=lambda item: item[0])[-1][1]
    return latest if _is_valid_number(latest) else None


def _last_annual_date(current: date) -> date:
    return date(current.year - 1, 12, 31)


def _last_same_period(current: date) -> date:
    return date(current.year - 1, current.month, current.day)


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _is_valid_number(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and isnan(value):
        return False
    return True
