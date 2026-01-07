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


@dataclass(frozen=True)
class TushareRtIdxKFetcher(Fetcher):
    """Fetch real-time index snapshots from Tushare rt_idx_k."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch all configured index snapshots in one call."""
        params = chunk_args.get("params") or {}
        codes = _require_codes(params.get("codes"))
        fields = "ts_code,open,close,high,low,pre_close,vol,amount"
        rows = self.retry_policy.execute(lambda: _safe_rt_idx_k(self.client, codes, fields))
        logger.info(
            "tushare rt_idx_k fetched",
            extra={"row_count": len(rows), "code_count": len(codes.split(","))},
        )
        return rows


def _safe_rt_idx_k(client: TushareClient, codes: str, fields: str) -> list[dict[str, object]]:
    try:
        return client.rt_idx_k(ts_code=codes, fields=fields)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_codes(value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value
    if isinstance(value, Iterable):
        codes = [item for item in value if isinstance(item, str) and item]
        if codes:
            return ",".join(codes)
    raise ValueError("codes must be a string or list of strings")
