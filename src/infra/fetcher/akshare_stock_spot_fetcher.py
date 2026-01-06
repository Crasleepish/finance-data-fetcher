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
class AkshareStockSpotFetcher(Fetcher):
    """Fetch real-time A-share spot data from Akshare."""

    retry_policy: RetryPolicy

    def fetch(self, _: ChunkArgs) -> RawBatch:
        """Fetch all stock spot rows."""
        rows = self.retry_policy.execute(_safe_spot)
        logger.info(
            "akshare stock spot fetched",
            extra={"row_count": len(rows)},
        )
        return rows


def _safe_spot() -> list[dict[str, object]]:
    try:
        import akshare as ak

        data = ak.stock_zh_a_spot_em()
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))
    except Exception as exc:
        raise RetryableError(str(exc)) from exc
