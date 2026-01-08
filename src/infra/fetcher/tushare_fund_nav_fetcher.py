from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,nav_date,unit_nav,adj_nav"


@dataclass(frozen=True)
class TushareFundNavFetcher(Fetcher):
    """Fetch fund_nav data for configured funds on a nav date."""

    client: TushareClient
    retry_policy: RetryPolicy
    batch_size: int = 6000

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_nav rows for each batch of fund codes."""
        params = chunk_args.get("params") or {}
        nav_date = _require_param(params, "nav_date")
        nav_date_compact = _to_yyyymmdd(nav_date)
        fields = str(params.get("fields", _DEFAULT_FIELDS))
        codes = _require_codes(params.get("codes"))

        rows: list[dict[str, object]] = []
        batches = _chunk_codes(codes, self.batch_size)
        for batch in batches:
            ts_code = ",".join(batch)
            batch_rows = self.retry_policy.execute(
                lambda: _safe_fund_nav(self.client, ts_code, nav_date_compact, fields)
            )
            rows.extend(batch_rows)
        logger.info(
            "tushare fund_nav fetched",
            extra={
                "nav_date": nav_date_compact,
                "fund_count": len(codes),
                "batch_count": len(batches),
                "row_count": len(rows),
            },
        )
        return rows


def _safe_fund_nav(
    client: TushareClient, ts_code: str, nav_date: str, fields: str
) -> list[dict[str, object]]:
    try:
        return client.fund_nav(ts_code=ts_code, nav_date=nav_date, fields=fields)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _require_codes(value: object) -> list[str]:
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError("codes must be list[str]")


def _chunk_codes(codes: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]


def _to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return value.replace("-", "")
