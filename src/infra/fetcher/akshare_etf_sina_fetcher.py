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
class AkshareEtfSinaFetcher(Fetcher):
    """Fetch real-time ETF quotes via Akshare Sina."""

    retry_policy: RetryPolicy

    def fetch(self, _: ChunkArgs) -> RawBatch:
        """Fetch ETF snapshot rows."""
        rows = self.retry_policy.execute(_safe_etf_sina)
        logger.info(
            "akshare etf sina fetched",
            extra={"row_count": len(rows)},
        )
        return rows


def _safe_etf_sina() -> list[dict[str, object]]:
    try:
        import akshare as ak

        data = ak.fund_etf_category_sina(symbol="ETF基金")
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))
    except Exception as exc:
        raise RetryableError(str(exc)) from exc
