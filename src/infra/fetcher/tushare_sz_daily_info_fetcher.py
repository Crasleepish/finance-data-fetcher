from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

# Tushare `sz_daily_info` (深圳市场每日交易概况) takes no fields/exchange
# arguments and returns at most 2000 rows per request (the source holds ~16
# category rows per trade day, so 50-trade-day chunks stay far below the cap;
# `_validate_chunk_size` below is a hard safety net). The source starts
# 2008-01-02; the pipeline never schedules earlier chunks. Raw financial
# fields are in yuan/shares and are scaled to 亿元/亿股 by the cleaner.
_MAX_ROWS_PER_REQUEST = 2000


@dataclass(frozen=True)
class TushareSzDailyInfoFetcher(Fetcher):
    """Fetch Tushare sz_daily_info (深圳市场每日交易概况) rows for a date range."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw sz_daily_info rows for the given date range chunk.

        The chunk period is expected to span at most 50 trade days (planned by
        the pipeline's TradeDayRangeChunkPolicy) and to start on or after
        2008-01-02; dates are validated and normalized to YYYYMMDD before
        calling the client. Returned rows are tagged with `_source: "sz"` so
        the cleaner can apply SZ-specific normalization.
        """
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")

        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        _validate_date_range(start_date_compact, end_date_compact)

        rows = self.retry_policy.execute(
            lambda: _safe_sz_daily_info(self.client, start_date_compact, end_date_compact)
        )
        _validate_chunk_size(rows)
        logger.info(
            "tushare sz_daily_info fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "row_count": len(rows),
            },
        )
        return [{**row, "_source": "sz"} for row in rows]


def _safe_sz_daily_info(
    client: TushareClient,
    start_date: str,
    end_date: str,
) -> list[dict[str, object]]:
    try:
        return client.sz_daily_info(start_date=start_date, end_date=end_date)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _validate_date_range(start_date: str, end_date: str) -> None:
    if not _is_yyyymmdd(start_date) or not _is_yyyymmdd(end_date):
        raise ValueError("dates must be in YYYYMMDD format")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")


def _validate_chunk_size(rows: list[dict[str, object]]) -> None:
    if len(rows) > _MAX_ROWS_PER_REQUEST:
        raise ValueError(
            f"sz_daily_info response exceeded per-request row limit "
            f"({_MAX_ROWS_PER_REQUEST}); request chunk must be reduced"
        )


def _to_yyyymmdd(value: str) -> str:
    if _is_yyyymmdd(value):
        return value
    return value.replace("-", "")


def _is_yyyymmdd(value: str) -> bool:
    return len(value) == 8 and value.isdigit()
