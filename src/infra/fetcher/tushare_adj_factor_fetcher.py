from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,trade_date,adj_factor"


@dataclass(frozen=True)
class TushareAdjFactorFetcher(Fetcher):
    """Fetch adj_factor data for a single trade date."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch adj_factor rows for the given trade date."""
        params = chunk_args.get("params") or {}
        trade_date = _require_param(params, "trade_date")
        trade_date_compact = _to_yyyymmdd(trade_date)
        fields = str(params.get("fields", _DEFAULT_FIELDS))

        rows = self.retry_policy.execute(
            lambda: _safe_adj_factor(self.client, trade_date_compact, fields)
        )
        logger.info(
            "tushare adj_factor fetched",
            extra={"trade_date": trade_date_compact, "row_count": len(rows)},
        )
        return rows


def _safe_adj_factor(
    client: TushareClient, trade_date: str, fields: str
) -> list[dict[str, object]]:
    try:
        return client.adj_factor(trade_date=trade_date, fields=fields)
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
