from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import cast

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AkshareIndexHistMinFetcher(Fetcher):
    """Fetch index 30-min data from Akshare."""

    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch min-level data for one index code."""
        params = chunk_args.get("params") or {}
        code = _require_param(params, "code")
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        rows = self.retry_policy.execute(lambda: _safe_index_min(code, start_date, end_date))
        logger.info(
            "akshare index min fetched",
            extra={"row_count": len(rows), "code": code},
        )
        return rows


def _safe_index_min(symbol: str, start_date: str, end_date: str) -> list[dict[str, object]]:
    try:
        import akshare as ak

        data = ak.stock_zh_index_hist_min_em(
            symbol=symbol,
            period="30",
            start_date=start_date,
            end_date=end_date,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value
