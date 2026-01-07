from __future__ import annotations

from models.task_spec import TaskSpec

TASK_PIPELINE_MAPPING = {
    TaskSpec.PIPELINE: ["dummy"],
    TaskSpec.GET_STOCK_INFO: ["stock_info"],
    TaskSpec.GET_STOCK_HIST_UNADJ: ["stock_hist_unadj"],
    TaskSpec.GET_FUNDAMENTAL_DATA: ["fundamental_data", "fundamental_data_single"],
    TaskSpec.GET_ADJ_FACTOR: ["adj_factor"],
    TaskSpec.GET_INDEX_INFO: ["index_info"],
    TaskSpec.GET_INDEX_HIST_STOCK: ["index_hist_stock"],
    TaskSpec.GET_INDEX_HIST_BOND: ["index_hist_bond"],
    TaskSpec.GET_INDEX_HIST_GOLD: ["index_hist_gold"],
    TaskSpec.GET_FUND_INFO: ["fund_info"],
    TaskSpec.GET_FUND_HIST_INDEX: ["fund_hist_index"],
    TaskSpec.GET_FUND_HIST_MONEY: ["fund_hist_money"],
    TaskSpec.GET_ETF_INFO: ["etf_info"],
    TaskSpec.GET_ETF_HIST: ["etf_hist"],
    TaskSpec.GET_MARKET_FACTORS: ["market_factors"],
    TaskSpec.GET_GOLD_CFTC_REPORT: ["gold_cftc_report"],
    TaskSpec.GET_GOLD_FUTURE_CURVE: ["gold_future_curve"],
    TaskSpec.GET_FUND_BETA: ["fund_beta"],
    TaskSpec.GET_RT_STOCK_HIST_UNADJ: [
        "rt_stock_hist_unadj_tushare",
        "rt_stock_hist_unadj_akshare",
    ],
    TaskSpec.GET_RT_INDEX_HIST: [
        "rt_index_hist_xueqiu",
        "rt_index_hist_akshare",
    ],
    TaskSpec.GET_RT_ETF_HIST: [
        "rt_etf_hist_akshare",
        "rt_etf_hist_xueqiu",
    ],
}
