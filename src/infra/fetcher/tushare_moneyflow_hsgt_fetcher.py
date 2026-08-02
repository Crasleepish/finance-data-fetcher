from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money"


@dataclass(frozen=True)
class TushareMoneyflowHsgtFetcher(Fetcher):
    """Fetch moneyflow_hsgt data for a date range."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw moneyflow_hsgt rows for the given date range."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        fields = str(params.get("fields", _DEFAULT_FIELDS))

        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        rows = self.retry_policy.execute(
            lambda: _safe_moneyflow_hsgt(
                self.client,
                start_date_compact,
                end_date_compact,
                fields,
            )
        )
        logger.info(
            "tushare moneyflow_hsgt fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "row_count": len(rows),
            },
        )
        return rows


def _safe_moneyflow_hsgt(
    client: TushareClient,
    start_date: str,
    end_date: str,
    fields: str,
) -> list[dict[str, object]]:
    try:
        return client.moneyflow_hsgt(
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


def _to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return value.replace("-", "")
