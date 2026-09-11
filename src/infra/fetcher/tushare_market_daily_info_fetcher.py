from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = (
    "trade_date,ts_code,ts_name,com_count,total_share,float_share,total_mv,"
    "float_mv,amount,vol,trans_count,pe,tr,exchange"
)
# Tushare `daily_info` (doc_id=215, 市场交易统计) returns at most 4000 rows per
# request. This fetcher serves the SH source only: it requests exchange='SH'
# and defensively drops any non-SH row from the response before cleaning.
# Pipelines chunk by trade days (<= 50 trade days per chunk, see
# MarketDailyInfoPipeline), keeping responses well below the cap;
# `_validate_chunk_size` below is a hard safety net against truncation.
_MAX_ROWS_PER_REQUEST = 4000
_SOURCE_EXCHANGE = "SH"


@dataclass(frozen=True)
class TushareMarketDailyInfoFetcher(Fetcher):
    """Fetch Tushare 市场交易统计 (pro.daily_info) SH rows for a date range."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw daily_info rows for the given date range chunk.

        The chunk period is expected to span at most 50 trade days (planned by
        the pipeline's TradeDayRangeChunkPolicy); dates are validated and
        normalized to YYYYMMDD before calling the client. Rows are requested
        with exchange='SH' and filtered defensively to SH-only before tagging
        with `_source: "sh"` for the cleaner.
        """
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        fields = str(params.get("fields", _DEFAULT_FIELDS))

        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        _validate_date_range(start_date_compact, end_date_compact)

        rows = self.retry_policy.execute(
            lambda: _safe_daily_info(
                self.client,
                start_date_compact,
                end_date_compact,
                fields,
            )
        )
        rows = [{**row, "_source": "sh"} for row in rows if row.get("exchange") == _SOURCE_EXCHANGE]
        _validate_chunk_size(rows)
        logger.info(
            "tushare daily_info fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "row_count": len(rows),
            },
        )
        return rows


def _safe_daily_info(
    client: TushareClient,
    start_date: str,
    end_date: str,
    fields: str,
) -> list[dict[str, object]]:
    try:
        return client.daily_info(
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            exchange=_SOURCE_EXCHANGE,
        )
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
            f"daily_info response exceeded per-request row limit ({_MAX_ROWS_PER_REQUEST}); "
            "request chunk must be reduced"
        )


def _to_yyyymmdd(value: str) -> str:
    if _is_yyyymmdd(value):
        return value
    return value.replace("-", "")


def _is_yyyymmdd(value: str) -> bool:
    return len(value) == 8 and value.isdigit()
