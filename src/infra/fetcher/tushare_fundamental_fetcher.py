from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_INCOME_FIELDS = (
    "ts_code,end_date,n_income_attr_p,n_income,continued_net_profit,end_net_profit,"
    "operate_profit,total_revenue,total_cogs,oper_exp"
)
_BALANCE_FIELDS = (
    "ts_code,end_date,total_hldr_eqy_exc_min_int,total_assets,total_cur_liab,total_ncl,total_liab"
)
_CASHFLOW_FIELDS = "ts_code,end_date,n_cashflow_act,c_pay_acq_const_fiolta"


@dataclass(frozen=True)
class TushareFundamentalFetcher(Fetcher):
    """Fetch income/balance/cashflow vip data for quarterly periods."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for the requested quarterly range."""
        cancel_check = _get_cancel_check(chunk_args)
        params = chunk_args.get("params") or {}
        start_period = _require_param(params, "start_period")
        end_period = _require_param(params, "end_period")
        overwrite = bool(params.get("overwrite", False))
        periods = _quarter_periods(start_period, end_period)

        payloads: list[dict[str, object]] = []
        for period in periods:
            if cancel_check is not None:
                cancel_check()
            income_rows = self.retry_policy.execute(
                lambda: _safe_call(self.client.income_vip, period, _INCOME_FIELDS)
            )
            balance_rows = self.retry_policy.execute(
                lambda: _safe_call(self.client.balancesheet_vip, period, _BALANCE_FIELDS)
            )
            cashflow_rows = self.retry_policy.execute(
                lambda: _safe_call(self.client.cashflow_vip, period, _CASHFLOW_FIELDS)
            )
            logger.info(
                "tushare fundamental fetched",
                extra={
                    "period": period,
                    "income_count": len(income_rows),
                    "balance_count": len(balance_rows),
                    "cashflow_count": len(cashflow_rows),
                },
            )
            payloads.append(
                {
                    "period": period,
                    "overwrite": overwrite,
                    "income": income_rows,
                    "balance": balance_rows,
                    "cashflow": cashflow_rows,
                }
            )
        return payloads


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


def _quarter_periods(start_period: str, end_period: str) -> list[str]:
    start = _parse_period(start_period)
    end = _parse_period(end_period)
    if start > end:
        raise ValueError("start_period must be <= end_period")
    periods: list[str] = []
    current = start
    while current <= end:
        periods.append(current.strftime("%Y%m%d"))
        current = _next_quarter_end(current)
    return periods


def _parse_period(value: str) -> date:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    if (parsed.month, parsed.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
        raise ValueError("period must be quarter end date")
    return parsed


def _next_quarter_end(value: date) -> date:
    if value.month == 3:
        return date(value.year, 6, 30)
    if value.month == 6:
        return date(value.year, 9, 30)
    if value.month == 9:
        return date(value.year, 12, 31)
    return date(value.year + 1, 3, 31)


def _get_cancel_check(chunk_args: ChunkArgs) -> Callable[[], None] | None:
    check = chunk_args.get("cancel_check")
    return check if callable(check) else None
