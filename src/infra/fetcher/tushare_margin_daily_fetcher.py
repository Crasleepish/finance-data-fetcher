from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "trade_date,exchange_id,rzye,rzmre,rzche,rqye,rqmcl,rzrqye,rqyl"
# Tushare `margin` returns at most 4000 rows per request. Pipelines chunk by trade
# days (<= 300 trade days per request) which keeps the response well below this cap.
_MAX_ROWS_PER_REQUEST = 4000


@dataclass(frozen=True)
class TushareMarginDailyFetcher(Fetcher):
    """Fetch Tushare margin daily-summary data for a date range."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw margin rows for the given date range."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        fields = str(params.get("fields", _DEFAULT_FIELDS))

        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        _validate_date_range(start_date_compact, end_date_compact)

        rows = self.retry_policy.execute(
            lambda: _safe_margin(
                self.client,
                start_date_compact,
                end_date_compact,
                fields,
            )
        )
        _validate_chunk_size(rows)
        logger.info(
            "tushare margin fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "row_count": len(rows),
            },
        )
        return rows


def _safe_margin(
    client: TushareClient,
    start_date: str,
    end_date: str,
    fields: str,
) -> list[dict[str, object]]:
    try:
        return client.margin(start_date=start_date, end_date=end_date, fields=fields)
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
            f"margin response exceeded per-request row limit ({_MAX_ROWS_PER_REQUEST}); "
            "request chunk must be reduced"
        )


def _to_yyyymmdd(value: str) -> str:
    if _is_yyyymmdd(value):
        return value
    return value.replace("-", "")


def _is_yyyymmdd(value: str) -> bool:
    return len(value) == 8 and value.isdigit()
