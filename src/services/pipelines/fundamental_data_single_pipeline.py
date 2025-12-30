from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import Engine, Table, select

from core.clean.fundamental_data_cleaner import FundamentalDataCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_fundamental_single_fetcher import TushareFundamentalSingleFetcher
from infra.tushare.client import TushareClient


@dataclass(frozen=True)
class FundamentalDataSinglePipeline(IngestionPipeline):
    """Pipeline for fetching fundamental data per-stock with non-vip endpoints."""

    client: TushareClient
    retry_policy: RetryPolicy
    engine: Engine
    stock_table: Table
    fundamental_table: Table
    _fetcher: TushareFundamentalSingleFetcher = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareFundamentalSingleFetcher(self.client, self.retry_policy),
        )

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan chunk args by grouping stock codes (100 per chunk)."""
        params = dict(arguments.get("params", {}))
        start_period = _require_param(params, "start_period")
        end_period = _require_param(params, "end_period")
        overwrite = bool(params.get("overwrite", False))

        stock_codes = _fetch_stock_codes(self.engine, self.stock_table)
        chunks: list[ChunkArgs] = []
        for i in range(0, len(stock_codes), 100):
            chunks.append(
                {
                    "params": {
                        "start_period": start_period,
                        "end_period": end_period,
                        "overwrite": overwrite,
                        "stock_codes": stock_codes[i : i + 100],
                    }
                }
            )
        return chunks

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for a stock-code chunk."""
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
        # Keep the TTM calculation identical to the vip pipeline.
        from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

        return _apply_operating_profit_ttm(
            records=records,
            engine=self.engine,
            table=self.fundamental_table,
            overwrite=overwrite,
        )


def _fetch_stock_codes(engine: Engine, table: Table) -> list[str]:
    stmt = select(table.c.stock_code).order_by(table.c.stock_code)
    with engine.begin() as connection:
        rows = connection.execute(stmt).scalars().all()
    return [row for row in rows if isinstance(row, str)]


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value
