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

_DEFAULT_FIELDS = "ts_code,trade_date,open,close,high,low,change,pct_change,vol,amount"


@dataclass(frozen=True)
class TushareSgeDailyFetcher(Fetcher):
    """Fetch sge_daily rows for configured contracts in a date range."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw sge_daily rows for each configured contract."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        fields = str(params.get("fields", _DEFAULT_FIELDS))
        codes = _require_codes(params.get("codes"))

        rows: list[dict[str, object]] = []
        for mapping in codes:
            batch = self.retry_policy.execute(
                lambda: _safe_sge_daily(
                    self.client,
                    mapping["api_code"],
                    start_date_compact,
                    end_date_compact,
                    fields,
                )
            )
            for item in batch:
                if isinstance(item, dict):
                    item["index_code"] = mapping["index_code"]
            rows.extend(batch)
        logger.info(
            "tushare sge_daily fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "index_count": len(codes),
                "row_count": len(rows),
            },
        )
        return rows


def _safe_sge_daily(
    client: TushareClient,
    ts_code: str,
    start_date: str,
    end_date: str,
    fields: str,
) -> list[dict[str, object]]:
    try:
        return client.sge_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
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
