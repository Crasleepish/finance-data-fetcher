from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, TypedDict

from core.fetch.fetcher import Fetcher
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


class EtfCodeMapping(TypedDict):
    etf_code: str
    api_code: str


@dataclass
class PysnowballEtfQuotecFetcher(Fetcher):
    """Fetch real-time ETF snapshots via pysnowball.quotec."""

    token_fetcher: XueqiuTokenFetcher
    _refresh_attempted: bool = False

    def reset_refresh_state(self) -> None:
        """Reset refresh attempt state for a new pipeline run."""
        self._refresh_attempted = False

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch ETF snapshots for one chunk."""
        params = chunk_args.get("params") or {}
        mappings = _require_mappings(params.get("codes"))
        token_info = load_latest_token()
        if token_info is None or token_age_days(token_info[1]) > TOKEN_REFRESH_DAYS:
            if self._refresh_attempted:
                raise RuntimeError("xq_a_token refresh already attempted")
            token_info = refresh_token(self.token_fetcher)
            self._refresh_attempted = True

        token, token_time = token_info
        try:
            return _fetch_with_token(token, mappings)
        except Exception as exc:
            if (
                not self._refresh_attempted
                and token_age_seconds(token_time) > TOKEN_ERROR_REFRESH_SECONDS
            ):
                token, _refreshed_time = refresh_token(self.token_fetcher)
                self._refresh_attempted = True
                return _fetch_with_token(token, mappings)
            raise RuntimeError(f"pysnowball quotec failed: {exc}") from exc


def _fetch_with_token(token: str, mappings: Iterable[EtfCodeMapping]) -> list[dict[str, object]]:
    import pysnowball as ball

    mapping_list = list(mappings)
    if not mapping_list:
        return []
    codes_csv = ",".join(mapping["api_code"] for mapping in mapping_list)
    code_lookup = {mapping["api_code"].upper(): mapping["etf_code"] for mapping in mapping_list}

    ball.set_token(f"xq_a_token={token};")
    payload = ball.quotec(codes_csv)
    if not isinstance(payload, dict) or payload.get("error_code") not in (0, None):
        raise RuntimeError("quotec returned error")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise RuntimeError("quotec missing data")

    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            continue
        etf_code = code_lookup.get(symbol.upper()) or _from_xueqiu_code(symbol)
        rows.append(
            {
                "etf_code": etf_code,
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
        "pysnowball etf quotec fetched",
        extra={"row_count": len(rows)},
    )
    return rows


def _require_mappings(value: object) -> list[EtfCodeMapping]:
    if isinstance(value, list) and all(_is_mapping(item) for item in value):
        return value
    raise ValueError("codes must be list[EtfCodeMapping]")


def _is_mapping(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    return isinstance(value.get("etf_code"), str) and isinstance(value.get("api_code"), str)


def _from_xueqiu_code(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if len(symbol) < 3:
        return symbol
    prefix = symbol[:2]
    code = symbol[2:]
    if prefix == "SH":
        return f"{code}.SH"
    if prefix == "SZ":
        return f"{code}.SZ"
    return symbol
