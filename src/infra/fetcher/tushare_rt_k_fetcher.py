from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

DEFAULT_TS_CODE_PATTERNS = ["3*.SZ", "6*.SH", "0*.SZ", "9*.BJ"]
PAGE_LIMIT = 6000


@dataclass(frozen=True)
class TushareRtKFetcher(Fetcher):
    """Fetch real-time daily stock snapshots from Tushare rt_k."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch all real-time rows using wildcard codes."""
        params = chunk_args.get("params") or {}
        ts_codes = _normalize_codes(params.get("ts_codes"))
        fields = "ts_code,open,close,high,low,pre_close,vol,amount"
        rows: list[dict[str, object]] = []
        offset = 0
        while True:
            batch = self.retry_policy.execute(
                lambda: _safe_rt_k(self.client, ts_codes, fields, offset, PAGE_LIMIT)
            )
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
        logger.info(
            "tushare rt_k fetched",
            extra={"row_count": len(rows), "offset": offset},
        )
        return rows


def _safe_rt_k(
    client: TushareClient, ts_codes: str, fields: str, offset: int, limit: int
) -> list[dict[str, object]]:
    try:
        return client.rt_k(ts_code=ts_codes, fields=fields, offset=offset, limit=limit)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _normalize_codes(value: object) -> str:
    if value is None:
        return ",".join(DEFAULT_TS_CODE_PATTERNS)
    if isinstance(value, str) and value.strip():
        return value
    if isinstance(value, Iterable):
        codes = [item for item in value if isinstance(item, str) and item]
        if codes:
            return ",".join(codes)
    raise ValueError("ts_codes must be a string or list of strings")
