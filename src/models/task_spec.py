from __future__ import annotations

from enum import StrEnum


class TaskSpec(StrEnum):
    """Enumerated task specifications used by the task runtime."""

    NOOP_SLEEP = "noop_sleep"
    PIPELINE = "pipeline"
    GET_STOCK_INFO = "get_stock_info"
    GET_STOCK_HIST_UNADJ = "get_stock_hist_unadj"
    GET_FUNDAMENTAL_DATA = "get_fundamental_data"
    GET_ADJ_FACTOR = "get_adj_factor"
    GET_INDEX_INFO = "get_index_info"
    GET_INDEX_HIST_STOCK = "get_index_hist_stock"
    GET_INDEX_HIST_BOND = "get_index_hist_bond"
    GET_INDEX_HIST_GOLD = "get_index_hist_gold"
    GET_FUND_INFO = "get_fund_info"
