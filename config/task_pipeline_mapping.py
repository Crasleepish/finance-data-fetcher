from __future__ import annotations

from models.task_spec import TaskSpec

TASK_PIPELINE_MAPPING = {
    TaskSpec.PIPELINE: ["dummy"],
    TaskSpec.GET_STOCK_INFO: ["stock_info"],
    TaskSpec.GET_STOCK_HIST_UNADJ: ["stock_hist_unadj"],
}
