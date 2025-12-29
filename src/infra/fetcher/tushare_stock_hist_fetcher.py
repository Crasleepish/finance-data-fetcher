from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DAILY_FIELDS = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
_DAILY_BASIC_FIELDS = (
    "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,"
    "dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
)


@dataclass(frozen=True)
class TushareStockHistUnadjFetcher(Fetcher):
    """Fetch daily, daily_basic, stock_st, and suspend data for a trade date."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for a trade date."""
        params = chunk_args.get("params") or {}
        trade_date = _require_param(params, "trade_date")
        trade_date_compact = _to_yyyymmdd(trade_date)

        daily_rows = self.retry_policy.execute(
            lambda: _safe_call(self.client.daily, trade_date_compact, _DAILY_FIELDS)
        )
        daily_basic_rows = self.retry_policy.execute(
            lambda: _safe_call(self.client.daily_basic, trade_date_compact, _DAILY_BASIC_FIELDS)
        )
        stock_st_rows = self.retry_policy.execute(
            lambda: _safe_call(self.client.stock_st, trade_date_compact, "ts_code")
        )
        suspend_rows = self.retry_policy.execute(
            lambda: _safe_call(self.client.suspend_d, trade_date_compact, "S", "ts_code")
        )

        logger.info(
            "tushare stock_hist fetched",
            extra={
                "trade_date": trade_date_compact,
                "daily_count": len(daily_rows),
                "daily_basic_count": len(daily_basic_rows),
                "stock_st_count": len(stock_st_rows),
                "suspend_count": len(suspend_rows),
            },
        )

        return [
            {
                "trade_date": trade_date_compact,
                "daily": daily_rows,
                "daily_basic": daily_basic_rows,
                "stock_st": stock_st_rows,
                "suspend": suspend_rows,
            }
        ]


def _safe_call(func, *args):  # type: ignore[no-untyped-def]
    try:
        return func(*args)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return value.replace("-", "")
