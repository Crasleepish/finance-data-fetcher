from __future__ import annotations

from enum import StrEnum


class TaskSpec(StrEnum):
    """Enumerated task specifications used by the task runtime."""

    NOOP_SLEEP = "noop_sleep"
    PIPELINE = "pipeline"
    GET_STOCK_INFO = "get_stock_info"
    GET_STOCK_HIST_UNADJ = "get_stock_hist_unadj"
    GET_FUNDAMENTAL_DATA = "get_fundamental_data"
