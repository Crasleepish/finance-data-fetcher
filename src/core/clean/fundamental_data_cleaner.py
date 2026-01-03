from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from math import isnan
from typing import Any, Iterable

from core.pipeline.types import NormalizedBatch, RawBatch


@dataclass(frozen=True)
class FundamentalDataCleaner:
    """Normalize income/balance/cashflow data into fundamental_data records."""

    overwrite: bool = False

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Convert raw quarterly payloads into DB-ready records."""
        payloads = _as_payloads(raw_batch)
        records: list[dict[str, Any]] = []
        for payload in payloads:
            income = _dedupe_last(_as_rows(payload.get("income")))
            balance = _dedupe_last(_as_rows(payload.get("balance")))
            cashflow = _dedupe_last(_as_rows(payload.get("cashflow")))
            keys = set(income) | set(balance) | set(cashflow)
            for key in keys:
                income_row = income.get(key, {})
                balance_row = balance.get(key, {})
                cashflow_row = cashflow.get(key, {})
                record = _build_record(
                    income_row=income_row,
                    balance_row=balance_row,
                    cashflow_row=cashflow_row,
                    overwrite=self.overwrite,
                )
                if record["stock_code"] is None or record["report_date"] is None:
                    continue
                records.append(record)
        return records


def _build_record(
    income_row: dict[str, Any],
    balance_row: dict[str, Any],
    cashflow_row: dict[str, Any],
    overwrite: bool,
) -> dict[str, Any]:
    ts_code = _first_non_empty(
        income_row.get("ts_code"),
        balance_row.get("ts_code"),
        cashflow_row.get("ts_code"),
    )
    end_date = _parse_date(
        _first_non_empty(
            income_row.get("end_date"),
            balance_row.get("end_date"),
            cashflow_row.get("end_date"),
        )
    )
    total_equity = _parse_amount(balance_row.get("total_hldr_eqy_exc_min_int"))
    total_assets = _parse_amount(balance_row.get("total_assets"))
    current_liabilities = _parse_amount(balance_row.get("total_cur_liab"))
    noncurrent_liabilities = _parse_amount(balance_row.get("total_ncl"))
    total_liabilities = _parse_amount(balance_row.get("total_liab"))

    # Prefer a safe total_liabilities: use max(cur+ncur, total_liab) when missing or overwrite.
    # Keep None when all inputs are missing to avoid writing 0 for absent data.
    if overwrite or _is_missing(total_liabilities):
        sum_parts = None
        if not (_is_missing(current_liabilities) and _is_missing(noncurrent_liabilities)):
            sum_parts = _safe_num(current_liabilities) + _safe_num(noncurrent_liabilities)
        candidates = [value for value in (total_liabilities, sum_parts) if not _is_missing(value)]
        total_liabilities = max(candidates) if candidates else None

    net_profit = _net_profit(income_row)
    operating_profit = _parse_amount(income_row.get("operate_profit"))
    total_revenue = _parse_amount(income_row.get("total_revenue"))
    total_cost = _first_non_none(
        _parse_amount(income_row.get("total_cogs")),
        _parse_amount(income_row.get("oper_exp")),
    )
    net_cash_from_operating = _parse_amount(cashflow_row.get("n_cashflow_act"))
    cash_for_fixed_assets = _parse_amount(cashflow_row.get("c_pay_acq_const_fiolta"))

    return {
        "stock_code": ts_code,
        "report_date": end_date,
        "total_equity": total_equity,
        "total_assets": total_assets,
        "current_liabilities": current_liabilities,
        "noncurrent_liabilities": noncurrent_liabilities,
        "net_profit": net_profit,
        "operating_profit": operating_profit,
        "total_revenue": total_revenue,
        "total_cost": total_cost,
        "net_cash_from_operating": net_cash_from_operating,
        "cash_for_fixed_assets": cash_for_fixed_assets,
        "operating_profit_ttm": None,
        "total_liabilities": total_liabilities,
    }


def _net_profit(row: dict[str, Any]) -> float | None:
    # Prefer attributable net profit, then net income, else continued+end net profit (only if both present).
    continued = _parse_amount(row.get("continued_net_profit"))
    end_net = _parse_amount(row.get("end_net_profit"))
    fallback = None
    if continued is not None and end_net is not None:
        fallback = _safe_num(continued) + _safe_num(end_net)
    return _first_non_none(
        _parse_amount(row.get("n_income_attr_p")),
        _parse_amount(row.get("n_income")),
        fallback,
    )


def _dedupe_last(rows: list[dict[str, Any]]) -> dict[tuple[str, date], dict[str, Any]]:
    # Keep the last record per (ts_code, end_date) to align with "last wins" rule.
    result: dict[tuple[str, date], dict[str, Any]] = {}
    for row in rows:
        ts_code = row.get("ts_code")
        end_date = _parse_date(row.get("end_date"))
        if isinstance(ts_code, str) and ts_code and isinstance(end_date, date):
            result[(ts_code, end_date)] = row
    return result


def _as_payloads(raw_batch: RawBatch) -> Iterable[dict[str, Any]]:
    if not raw_batch:
        return []
    if isinstance(raw_batch, list):
        return [item for item in raw_batch if isinstance(item, dict)]
    raise ValueError("raw batch must be a list of dict payloads")


def _as_rows(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    raise ValueError("expected list of dict rows")


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        if len(value) == 10:
            return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("invalid date value")


def _parse_amount(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and isnan(value):
            return None
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _safe_num(value: float | None) -> float:
    return 0.0 if value is None else value


def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and isnan(value))


def _first_non_none(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None
