from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,trade_date,open,high,low,close,change,pct_chg,vol,amount"


@dataclass(frozen=True)
class TushareFundDailyFetcher(Fetcher):
    """Fetch fund_daily data for a trade date with pagination."""

    client: TushareClient
    retry_policy: RetryPolicy
    page_size: int = 4000

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_daily rows for a trade date."""
        params = chunk_args.get("params") or {}
        trade_date = _require_param(params, "trade_date")
        trade_date_compact = _to_yyyymmdd(trade_date)
        fields = str(params.get("fields", _DEFAULT_FIELDS))
        codes = _require_codes(params.get("codes"))

        all_rows: list[dict[str, object]] = []
        offset = 0
        pages = 0
        while True:
            batch = self.retry_policy.execute(
                lambda: _safe_fund_daily(
                    self.client,
                    trade_date_compact,
                    fields,
                    offset,
                    self.page_size,
                )
            )
            if not batch:
                logger.info(
                    "tushare fund_daily exhausted",
                    extra={
                        "trade_date": trade_date_compact,
                        "pages": pages,
                        "total_rows": len(all_rows),
                    },
                )
                break
            all_rows.extend(batch)
            pages += 1
            logger.debug(
                "tushare fund_daily page",
                extra={
                    "trade_date": trade_date_compact,
                    "offset": offset,
                    "limit": self.page_size,
                    "batch_size": len(batch),
                },
            )
            offset += self.page_size

        if not codes:
            return all_rows
        code_set = set(codes)
        filtered = [
            row
            for row in all_rows
            if isinstance(row, dict)
            and isinstance(row.get("ts_code"), str)
            and row.get("ts_code") in code_set
        ]
        logger.info(
            "tushare fund_daily filtered",
            extra={
                "trade_date": trade_date_compact,
                "input_rows": len(all_rows),
                "filtered_rows": len(filtered),
                "code_count": len(code_set),
            },
        )
        return filtered


def _safe_fund_daily(
    client: TushareClient,
    trade_date: str,
    fields: str,
    offset: int,
    limit: int,
) -> list[dict[str, object]]:
    try:
        return client.fund_daily(
            trade_date=trade_date,
            fields=fields,
            offset=offset,
            limit=limit,
        )
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _require_codes(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError("codes must be list[str]")


def _to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return value.replace("-", "")
