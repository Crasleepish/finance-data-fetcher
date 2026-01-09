from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.indexing.index_codes import IndexCodeMapping
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,trade_date,open,close,high,low,pre_close,change,pct_chg,vol,amount"


@dataclass(frozen=True)
class TushareIndexDailyFetcher(Fetcher):
    """Fetch index_daily rows for configured indices within a date range."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index_daily rows for each configured index code."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        fields = str(params.get("fields", _DEFAULT_FIELDS))
        codes = _require_codes(params.get("codes"))
        limit = _require_limit(params.get("limit", 6000))

        rows: list[dict[str, object]] = []
        for mapping in codes:
            offset = 0
            while True:
                batch = self.retry_policy.execute(
                    lambda: _safe_index_daily(
                        self.client,
                        mapping["api_code"],
                        start_date_compact,
                        end_date_compact,
                        fields,
                        offset,
                        limit,
                    )
                )
                if not batch:
                    break
                for item in batch:
                    if isinstance(item, dict):
                        item["index_code"] = mapping["index_code"]
                rows.extend(batch)
                offset += limit
        logger.info(
            "tushare index_daily fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "index_count": len(codes),
                "row_count": len(rows),
            },
        )
        return rows


def _safe_index_daily(
    client: TushareClient,
    ts_code: str,
    start_date: str,
    end_date: str,
    fields: str,
    offset: int,
    limit: int,
) -> list[dict[str, object]]:
    try:
        return client.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
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


def _require_codes(value: object) -> list[IndexCodeMapping]:
    if isinstance(value, list) and all(_is_code_mapping(item) for item in value):
        return value
    raise ValueError("codes must be list[IndexCodeMapping]")


def _is_code_mapping(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    return isinstance(value.get("index_code"), str) and isinstance(value.get("api_code"), str)


def _to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return value.replace("-", "")


def _require_limit(value: object) -> int:
    if isinstance(value, int) and value > 0:
        return value
    raise ValueError("limit must be positive integer")
