from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

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
class TushareFundamentalSingleFetcher(Fetcher):
    """Fetch income/balance/cashflow data for a list of stock codes."""

    client: TushareClient
    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for stock codes over the quarterly range."""
        params = chunk_args.get("params") or {}
        start_period = _require_param(params, "start_period")
        end_period = _require_param(params, "end_period")
        stock_codes = _require_list(params, "stock_codes")
        overwrite = bool(params.get("overwrite", False))
        start_date = _to_yyyymmdd(start_period)
        end_date = _to_yyyymmdd(end_period)

        income_rows: list[dict[str, object]] = []
        balance_rows: list[dict[str, object]] = []
        cashflow_rows: list[dict[str, object]] = []
        for stock_code in stock_codes:
            income_rows.extend(
                self.retry_policy.execute(
                    _build_call(
                        self.client.income,
                        stock_code,
                        start_date,
                        end_date,
                        _INCOME_FIELDS,
                    )
                )
            )
            balance_rows.extend(
                self.retry_policy.execute(
                    _build_call(
                        self.client.balancesheet,
                        stock_code,
                        start_date,
                        end_date,
                        _BALANCE_FIELDS,
                    )
                )
            )
            cashflow_rows.extend(
                self.retry_policy.execute(
                    _build_call(
                        self.client.cashflow,
                        stock_code,
                        start_date,
                        end_date,
                        _CASHFLOW_FIELDS,
                    )
                )
            )

        logger.info(
            "tushare fundamental single fetched",
            extra={
                "stock_count": len(stock_codes),
                "income_count": len(income_rows),
                "balance_count": len(balance_rows),
                "cashflow_count": len(cashflow_rows),
            },
        )

        return [
            {
                "overwrite": overwrite,
                "income": income_rows,
                "balance": balance_rows,
                "cashflow": cashflow_rows,
            }
        ]


def _safe_call(func, *args):  # type: ignore[no-untyped-def]
    try:
        return func(*args)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _build_call(func, *args):  # type: ignore[no-untyped-def]
    def _call():  # type: ignore[no-untyped-def]
        return _safe_call(func, *args)

    return _call


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _require_list(params: dict[str, object], key: str) -> list[str]:
    value = params.get(key)
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        raise ValueError(f"missing required param: {key}")
    return [str(item) for item in value if str(item)]


def _to_yyyymmdd(value: str) -> str:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    if (parsed.month, parsed.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
        raise ValueError("period must be quarter end date")
    return parsed.strftime("%Y%m%d")
