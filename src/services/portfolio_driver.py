from __future__ import annotations

import gc
import logging
import os
from typing import cast

import pandas as pd
from sqlalchemy.engine import Engine

from core.backtest.backtest_engine import BacktestConfig, run_backtest
from core.backtest.rebalance_date_generator import RebalanceDateGenerator
from core.backtest.stock_selector import (
    AmountSelector,
    BasicSelector,
    BMScoreSelector,
    MktCapPercentileSelector,
    QualityScoreSelector,
    Selector,
)
from core.backtest.weight_allocator import MktCapWeightAllocator
from infra.factor_data_fetcher import CalendarFetcher, DataFetcher

logger = logging.getLogger(__name__)


def read_from_csv(
    file_path: str,
    date_col: list[str] | str = "date",
    index_cols: list[int] = [0],
) -> pd.DataFrame:
    """Read CSV with consistent dtypes and date parsing."""
    df = pd.read_csv(
        file_path,
        index_col=index_cols,
        header=0,
        parse_dates=[date_col],
        date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d"),
        true_values=["True"],
        false_values=["False"],
    )
    df = df.astype("float32", errors="ignore")
    return df


def build_all_portfolios(start_date: str, end_date: str, mode: str, engine: Engine) -> None:
    """Build and backtest all portfolios for the given date range."""
    output_path = "./bt_result"
    os.makedirs(output_path, exist_ok=True)

    fetcher = DataFetcher(engine)
    stock_info = fetcher.get_stock_info_df()
    blacklist: list[str] = []

    prev_start_date = CalendarFetcher(engine).get_prev_trade_date(
        start_date.replace("-", ""), format="%Y-%m-%d"
    )

    logging.info("开始初始化数据...")
    logging.info("获取收盘价数据... %s - %s", prev_start_date, end_date)
    price = fetcher.fetch_adj_hist("close", prev_start_date, end_date)
    logging.info("获取市值数据... %s - %s", prev_start_date, end_date)
    mkt_cap = fetcher.fetch_price("mkt_cap", prev_start_date, end_date)
    logging.info("获取成交额数据... %s - %s", prev_start_date, end_date)
    amount = fetcher.fetch_price("amount", prev_start_date, end_date)
    logging.info("获取基本面数据... %s - %s", prev_start_date, end_date)
    fundamentals = fetcher.fetch_fundamentals_on_all(
        prev_start_date,
        end_date,
        fields=[
            "total_equity",
            "operating_profit_ttm",
            "total_assets",
            "total_liabilities",
            "net_profit",
            "net_cash_from_operating",
        ],
    )
    price.to_csv(os.path.join(output_path, "price.csv"))
    mkt_cap.to_csv(os.path.join(output_path, "mkt_cap.csv"))
    amount.to_csv(os.path.join(output_path, "amount.csv"))
    fundamentals.to_csv(os.path.join(output_path, "fundamentals.csv"))

    shared_data = {"price": price, "mkt_cap": mkt_cap, "fundamental": fundamentals}

    configs = [
        (
            "SL",
            "bm",
            (0.0, 0.5),
            (0.0, 0.3),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "SM",
            "bm",
            (0.0, 0.5),
            (0.3, 0.7),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "SH",
            "bm",
            (0.0, 0.5),
            (0.7, 1.0),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "BL",
            "bm",
            (0.5, 1.0),
            (0.0, 0.3),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "BM",
            "bm",
            (0.5, 1.0),
            (0.3, 0.7),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "BH",
            "bm",
            (0.5, 1.0),
            (0.7, 1.0),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "SL",
            "qmj",
            (0.0, 0.5),
            (0.0, 0.3),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "SM",
            "qmj",
            (0.0, 0.5),
            (0.3, 0.7),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "SH",
            "qmj",
            (0.0, 0.5),
            (0.7, 1.0),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "BL",
            "qmj",
            (0.5, 1.0),
            (0.0, 0.3),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "BM",
            "qmj",
            (0.5, 1.0),
            (0.3, 0.7),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
        (
            "BH",
            "qmj",
            (0.5, 1.0),
            (0.7, 1.0),
            {"freq": "custom_months", "custom_months": [3, 6, 9, 12], "anchor": "end"},
        ),
    ]

    for name, factor, size_percentile, score_percentile, rb_cfg in configs:
        logging.info(">>> Building portfolio: %s (%s)", name, factor)

        freq = cast(str, rb_cfg["freq"])
        anchor = cast(str, rb_cfg.get("anchor", "start"))
        custom_months = cast(list[int] | None, rb_cfg.get("custom_months"))
        rebalancer = RebalanceDateGenerator(
            freq=freq,
            anchor=anchor,
            custom_months=custom_months,
            calendar_fetcher=CalendarFetcher(engine),
        )
        rebalance_dates = rebalancer.get_dates_from_range(start_date, end_date)

        allocator = MktCapWeightAllocator()

        prev_date = rebalancer.get_prev_balance_date(start_date)
        weight_path = os.path.join(output_path, f"{factor}_{name}_weights.csv")
        hist_not_exists_flag = False
        use_hist_weight_flag = False
        if os.path.exists(weight_path):
            hist_weight = pd.read_csv(weight_path, index_col=0, parse_dates=True)
            if prev_date in hist_weight.index:
                use_hist_weight_flag = True
                init_weight = hist_weight.loc[prev_date]
                logging.info(
                    "%s 中存在上一个调仓日 %s 的权重数据，使用历史数据建仓",
                    weight_path,
                    prev_date,
                )
            else:
                hist_not_exists_flag = True
        else:
            hist_not_exists_flag = True

        if hist_not_exists_flag:
            use_hist_weight_flag = False
            init_date = prev_start_date
            rebalance_dates = pd.DatetimeIndex(
                [pd.to_datetime(init_date)] + rebalance_dates.tolist()
            )
            logging.info(
                "%s 中不存在上一个调仓日 %s 的权重数据，将%s加入再平衡日",
                weight_path,
                prev_date,
                init_date,
            )
            logging.warning("将%s加入再平衡日，这会导致%s日的收益数据为0", init_date, init_date)

        records = []
        all_stocks = set(stock_info.index)
        for dt in rebalance_dates:
            basic_selector = BasicSelector(stock_info, blacklist, shared_data.get("price"), dt)
            basic_amount_selector = AmountSelector(amount, dt, 0.01, parents=[basic_selector])
            size_selector = MktCapPercentileSelector(
                shared_data.get("mkt_cap"),
                dt,
                size_percentile,
            )
            lookback_range = 0 if mode == "realtime" else 90
            selector: Selector
            if factor == "bm":
                selector = BMScoreSelector(
                    fundamental_df=shared_data.get("fundamental"),
                    mkt_cap_df=shared_data.get("mkt_cap"),
                    asof_date=dt,
                    bm_percentile=score_percentile,
                    lookback_range=lookback_range,
                    parents=[basic_amount_selector, size_selector],
                )
            elif factor == "qmj":
                selector = QualityScoreSelector(
                    stock_info=stock_info,
                    fundamental_df=shared_data.get("fundamental"),
                    asof_date=dt,
                    score_percentile=score_percentile,
                    lookback_range=lookback_range,
                    parents=[basic_amount_selector, size_selector],
                )
            else:
                raise ValueError("Unknown factor")

            selected = selector.select(all_stocks)
            weights = allocator.allocate(selected, shared_data, dt)
            for stock, weight in weights.items():
                records.append((dt, stock, weight))

        if records:
            weight_df = pd.DataFrame(records, columns=["date", "stock_code", "weight"])
            weight_df = weight_df.pivot(
                index="date",
                columns="stock_code",
                values="weight",
            ).sort_index()
            weight_path = os.path.join(output_path, f"{factor}_{name}_weights.csv")
            if os.path.exists(weight_path):
                old_weight = pd.read_csv(weight_path, index_col=0, parse_dates=True)
                weight_df_full = pd.concat([old_weight, weight_df])
                weight_df_full = weight_df_full[
                    ~weight_df_full.index.duplicated(keep="last")
                ].sort_index()
                weight_df_full.to_csv(weight_path)
                del weight_df_full
            else:
                weight_df.to_csv(weight_path)

            if use_hist_weight_flag:
                weight_df = pd.concat(
                    [
                        pd.DataFrame(
                            [init_weight],
                            index=[pd.to_datetime(prev_start_date)],
                        ),
                        weight_df,
                    ]
                )
        else:
            weight_df = pd.DataFrame([init_weight], index=[pd.to_datetime(prev_start_date)])

        del records

        cfg = BacktestConfig(
            init_cash=100_000_000,
            buy_fee=0.0,
            sell_fee=0.0,
            slippage=0.0,
            cash_sharing=True,
        )
        result = run_backtest(weight_df, shared_data.get("price"), cfg)
        returns = cast(pd.Series, result["returns"])
        start_ts = pd.to_datetime(start_date)
        daily_return = returns.loc[start_ts:]
        daily_return.name = "value"
        return_path = os.path.join(output_path, f"{factor}_{name}_daily_returns.csv")
        daily_return.to_csv(return_path, index_label="date")

        del weight_df, result
        gc.collect()
