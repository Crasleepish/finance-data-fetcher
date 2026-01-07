from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from core.fetch.fetcher import Fetcher
from core.indexing.index_codes import IndexCodeMapping
from core.pipeline.types import ChunkArgs, RawBatch
from infra.xueqiu_token_fetcher import PROJECT_ROOT, XueqiuTokenFetcher

logger = logging.getLogger(__name__)

TOKEN_PREFIX = "xq_a_token_"
TOKEN_SUFFIX = ".txt"
TOKEN_REFRESH_DAYS = 7
TOKEN_ERROR_REFRESH_SECONDS = 60


@dataclass(frozen=True)
class PysnowballQuotecFetcher(Fetcher):
    """Fetch real-time index snapshots via pysnowball.quotec."""

    token_fetcher: XueqiuTokenFetcher
    codes: list[IndexCodeMapping]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch index snapshots for all configured codes."""
        _ = chunk_args
        token_info = _load_latest_token()
        refreshed = False
        if token_info is None or _age_days(token_info[1]) > TOKEN_REFRESH_DAYS:
            token_info = _refresh_token(self.token_fetcher)
            refreshed = True

        token, token_time = token_info
        try:
            return _fetch_with_token(token, self.codes)
        except Exception as exc:
            if not refreshed and _age_seconds(token_time) > TOKEN_ERROR_REFRESH_SECONDS:
                token, _refreshed_time = _refresh_token(self.token_fetcher)
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


def _load_latest_token() -> tuple[str, datetime] | None:
    latest: tuple[str, datetime] | None = None
    for path in PROJECT_ROOT.glob(f"{TOKEN_PREFIX}*{TOKEN_SUFFIX}"):
        timestamp = _parse_token_timestamp(path.name)
        if timestamp is None:
            continue
        if latest is None or timestamp > latest[1]:
            token = path.read_text(encoding="utf-8").strip()
            if token:
                latest = (token, timestamp)
    return latest


def _refresh_token(fetcher: XueqiuTokenFetcher) -> tuple[str, datetime]:
    path = fetcher.fetch_and_store()
    token = path.read_text(encoding="utf-8").strip()
    timestamp = _parse_token_timestamp(path.name)
    if not token or timestamp is None:
        raise RuntimeError("failed to refresh xq_a_token")
    return token, timestamp


def _parse_token_timestamp(name: str) -> datetime | None:
    if not name.startswith(TOKEN_PREFIX) or not name.endswith(TOKEN_SUFFIX):
        return None
    stem = name[len(TOKEN_PREFIX) : -len(TOKEN_SUFFIX)]
    try:
        return datetime.strptime(stem, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _age_days(token_time: datetime) -> int:
    return (datetime.now() - token_time).days


def _age_seconds(token_time: datetime) -> int:
    return int((datetime.now() - token_time).total_seconds())
