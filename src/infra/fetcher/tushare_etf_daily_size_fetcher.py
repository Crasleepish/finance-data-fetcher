from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from core.clean.etf_daily_size_cleaner import NAV_SOURCE, SHARE_SOURCE
from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

SHARE_MARKETS = ("SH", "SZ")
NAV_MARKET = "E"

_SHARE_FIELDS = "ts_code,trade_date,fd_share"
_NAV_FIELDS = "ts_code,nav_date,unit_nav,ann_date"

# Tushare fund_share (doc_id=207) returns at most 2000 rows per request; a
# response at the cap means the day's rows may be truncated silently.
MAX_SHARE_ROWS = 2000


@dataclass(frozen=True)
class TushareEtfDailySizeFetcher(Fetcher):
    """Fetch ETF fund_share (SH/SZ) and fund_nav (E) rows for one trade day."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch one chunk (trade date): SH shares, SZ shares, then E navs."""
        params = chunk_args.get("params") or {}
        trade_date = _require_param(params, "trade_date")
        rows: list[dict[str, object]] = []
        for market in SHARE_MARKETS:
            rows.extend(self.fetch_shares(trade_date, market))
        rows.extend(self.fetch_navs(trade_date))
        return rows

    def fetch_shares(self, trade_date: str | date, market: str) -> list[dict[str, object]]:
        """Fetch fund_share rows for one date/market, tagged `_source="share"`."""
        day = _to_yyyymmdd(trade_date)
        rows = self.retry_policy.execute(
            lambda: _safe_fund_share(self.client, day, market, _SHARE_FIELDS)
        )
        if len(rows) >= MAX_SHARE_ROWS:
            raise RuntimeError(
                f"fund_share returned {len(rows)} rows for {day} market {market}; "
                f"at or above the {MAX_SHARE_ROWS}-row request cap, data may be truncated"
            )
        logger.info(
            "tushare fund_share fetched",
            extra={"trade_date": day, "market": market, "row_count": len(rows)},
        )
        return [{**row, "_source": SHARE_SOURCE} for row in rows]

    def fetch_navs(self, nav_date: str | date) -> list[dict[str, object]]:
        """Fetch fund_nav rows for one nav date (market E), tagged `_source="nav"`."""
        day = _to_yyyymmdd(nav_date)
        rows = self.retry_policy.execute(
            lambda: _safe_fund_nav_market(self.client, day, NAV_MARKET, _NAV_FIELDS)
        )
        logger.info(
            "tushare fund_nav fetched",
            extra={"nav_date": day, "market": NAV_MARKET, "row_count": len(rows)},
        )
        return [{**row, "_source": NAV_SOURCE} for row in rows]


def _safe_fund_share(
    client: TushareClient, trade_date: str, market: str, fields: str
) -> list[dict[str, object]]:
    try:
        return client.fund_share(trade_date=trade_date, market=market, fields=fields)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _safe_fund_nav_market(
    client: TushareClient, nav_date: str, market: str, fields: str
) -> list[dict[str, object]]:
    try:
        return client.fund_nav_market(nav_date=nav_date, market=market, fields=fields)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _to_yyyymmdd(value: str | date) -> str:
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return value
        return value.replace("-", "")
    return value.strftime("%Y%m%d")
