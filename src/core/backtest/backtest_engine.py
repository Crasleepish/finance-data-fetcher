from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict

import numpy as np
import pandas as pd
import vectorbt as vbt
from numba import njit
from vectorbt.portfolio import nb
from vectorbt.portfolio.enums import Direction, SegmentContext, SizeType

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Centralised trade-execution assumptions."""

    buy_fee: float = 0.0005
    sell_fee: float = 0.0005
    slippage: float = 0.0002
    init_cash: float = 1_000_000.0
    cash_sharing: bool = True
    freq: str = "D"


def build_sparse_index_mapping(
    weight_index: pd.Index, price_index: pd.Index
) -> tuple[np.ndarray, np.ndarray]:
    """Build mapping from price index to weight index."""
    keys, values = [], []
    weight_pos = {dt: j for j, dt in enumerate(weight_index)}
    for i, dt in enumerate(price_index):
        if dt in weight_pos:
            keys.append(i)
            values.append(weight_pos[dt])
    return np.array(keys, dtype=np.int32), np.array(values, dtype=np.int32)


@njit(cache=True)
def lookup_index(i: int, keys: np.ndarray, values: np.ndarray) -> int:
    """Lookup price index mapping."""
    for k, v in zip(keys, values):
        if k == i:
            return int(v)
    return -1


@njit(cache=True)
def _pre_group_func_nb(c: Any) -> tuple[np.ndarray]:
    """Prepare group state for numba order functions."""
    order_value_out = np.empty(c.group_len, dtype=np.float64)
    return (order_value_out,)


@njit(cache=True)
def sparse_sort_call_seq_nb(
    c: SegmentContext,
    order_value_out: Any,
    target_w: Any,
    direction: Any,
    size_type: Any,
    price_idx_arr: Any,
    weight_idx_arr: Any,
) -> None:
    """Populate call sequence for numba order execution."""
    group_value_now = nb.get_group_value_ctx_nb(c)

    row_in_w = lookup_index(c.i, price_idx_arr, weight_idx_arr)
    if row_in_w == -1:
        return

    for k in range(c.from_col, c.to_col):
        col = k
        w = target_w[row_in_w, col]
        if np.isnan(w):
            w = 0.0

        cash_now = c.last_cash[c.group] if c.cash_sharing else c.last_cash[col]
        free_cash_now = c.last_free_cash[c.group] if c.cash_sharing else c.last_free_cash[col]
        val_price = c.last_val_price[col]

        order_value = nb.approx_order_value_nb(
            w,
            size_type,
            direction,
            cash_now,
            c.last_position[col],
            free_cash_now,
            val_price,
            group_value_now,
        )
        order_value_out[col - c.from_col] = order_value

    nb.insert_argsort_nb(order_value_out, c.call_seq_now)


@njit(cache=True)
def _pre_segment_func_nb(
    c: Any,
    order_value_out: Any,
    target_w: Any,
    price: Any,
    size_type: Any,
    direction: Any,
    price_idx_arr: Any,
    weight_idx_arr: Any,
) -> tuple[Any, ...]:
    """Pre-segment callback for numba order execution."""
    for col in range(c.from_col, c.to_col):
        c.last_val_price[col] = nb.get_col_elem_nb(c, col, price)
    sparse_sort_call_seq_nb(
        c,
        order_value_out,
        target_w,
        direction,
        size_type,
        price_idx_arr,
        weight_idx_arr,
    )
    return ()


@njit(cache=True)
def _order_func_nb(
    c: Any,
    target_w: Any,
    price: Any,
    buy_fee: float,
    sell_fee: float,
    slippage: float,
    price_idx_arr: Any,
    weight_idx_arr: Any,
) -> Any:
    """Generate target-percent order for each asset."""
    row_in_w = lookup_index(c.i, price_idx_arr, weight_idx_arr)
    if row_in_w == -1:
        return nb.order_nothing_nb()

    w = target_w[row_in_w, c.col]
    if np.isnan(w):
        return nb.order_nothing_nb()

    fee = buy_fee if w > c.position_now else sell_fee

    return nb.order_nb(
        size=w,
        size_type=SizeType.TargetPercent,
        price=price[c.i, c.col],
        fees=fee,
        slippage=slippage,
        direction=Direction.LongOnly,
    )


def run_backtest(
    weights: pd.DataFrame,
    close: pd.DataFrame,
    cfg: BacktestConfig | None = None,
) -> Dict[str, object]:
    """Run a weight-schedule backtest and return outputs."""
    if cfg is None:
        cfg = BacktestConfig()

    close = close.ffill().copy()
    weights = weights.fillna(0.0).copy()
    weights, close = weights.align(close, join="left", axis=1)
    if weights.empty or close.empty:
        raise ValueError("After alignment, weights/close share no common index or columns.")

    if weights.isna().all().all():
        raise ValueError("Weights are all NaN - nothing to do.")

    price_idx_arr, weight_idx_arr = build_sparse_index_mapping(weights.index, close.index)
    w_arr = weights.to_numpy(dtype=np.float64)
    price_arr = close.to_numpy(dtype=np.float64)
    seg_mask = np.isin(close.index, weights.index)[:, None]

    pf = vbt.Portfolio.from_order_func(
        close,
        _order_func_nb,
        w_arr,
        price_arr,
        cfg.buy_fee,
        cfg.sell_fee,
        cfg.slippage,
        price_idx_arr,
        weight_idx_arr,
        init_cash=cfg.init_cash,
        cash_sharing=cfg.cash_sharing,
        group_by=True,
        use_numba=True,
        freq=cfg.freq,
        segment_mask=seg_mask,
        pre_group_func_nb=_pre_group_func_nb,
        pre_segment_func_nb=_pre_segment_func_nb,
        pre_segment_args=(
            w_arr,
            price_arr,
            SizeType.TargetPercent,
            Direction.LongOnly,
            price_idx_arr,
            weight_idx_arr,
        ),
        ffill_val_price=True,
    )

    nav = pf.value()
    rets = pf.returns()
    aw = pf.asset_value(group_by=False).div(pf.value(), axis=0)
    err = aw.reindex_like(weights) - weights

    logging.info("===== Order Records =====")
    order_rec = pf.orders.records.copy()
    order_rec["timestamp"] = pf.wrapper.index[order_rec["idx"]]
    order_rec["asset"] = pf.wrapper.columns[order_rec["col"]]
    logging.info(order_rec[["timestamp", "asset", "size", "price", "fees", "side"]])

    stats = pf.returns_stats(defaults=dict(freq=cfg.freq))

    return {
        "pf": pf,
        "nav": nav,
        "returns": rets,
        "actual_weights": aw,
        "weight_error": err,
        "stats": stats,
    }
