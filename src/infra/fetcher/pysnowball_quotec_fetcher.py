from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from core.fetch.fetcher import Fetcher
from core.indexing.index_codes import IndexCodeMapping
from core.pipeline.types import ChunkArgs, RawBatch
from infra.xueqiu_token_cache import (
    TOKEN_ERROR_REFRESH_SECONDS,
    TOKEN_REFRESH_DAYS,
    load_latest_token,
    refresh_token,
    token_age_days,
    token_age_seconds,
)
from infra.xueqiu_token_fetcher import XueqiuTokenFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PysnowballQuotecFetcher(Fetcher):
    """Fetch real-time index snapshots via pysnowball.quotec."""

    token_fetcher: XueqiuTokenFetcher
    codes: list[IndexCodeMapping]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch index snapshots for all configured codes."""
        _ = chunk_args
        token_info = load_latest_token()
        refreshed = False
        if token_info is None or token_age_days(token_info[1]) > TOKEN_REFRESH_DAYS:
            token_info = refresh_token(self.token_fetcher)
            refreshed = True

        token, token_time = token_info
        try:
            return _fetch_with_token(token, self.codes)
        except Exception as exc:
            if not refreshed and token_age_seconds(token_time) > TOKEN_ERROR_REFRESH_SECONDS:
                token, _refreshed_time = refresh_token(self.token_fetcher)
                return _fetch_with_token(token, self.codes)
            raise RuntimeError(f"pysnowball quotec failed: {exc}") from exc


def _fetch_with_token(token: str, codes: Iterable[IndexCodeMapping]) -> list[dict[str, object]]:
    import pysnowball as ball

    ball.set_token(f"xq_a_token={token};")
    rows: list[dict[str, object]] = []
    for mapping in codes:
        payload = ball.quotec(mapping["api_code"])
        if not isinstance(payload, dict) or payload.get("error_code") not in (0, None):
            raise RuntimeError(f"quotec error for {mapping['api_code']}")
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise RuntimeError(f"quotec missing data for {mapping['api_code']}")
        item = data[0]
        if not isinstance(item, dict):
            raise RuntimeError(f"quotec invalid data for {mapping['api_code']}")
        rows.append(
            {
                "index_code": mapping["index_code"],
                "open": item.get("open"),
                "close": item.get("current"),
                "high": item.get("high"),
                "low": item.get("low"),
                "pre_close": item.get("last_close"),
                "volume": item.get("volume"),
                "amount": item.get("amount"),
            }
        )
    logger.info(
        "pysnowball quotec fetched",
        extra={"row_count": len(rows)},
    )
    return rows
