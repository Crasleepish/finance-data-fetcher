from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import Engine

from core.pipeline.types import RawBatch
from infra.db.tables import rt_stock_hist_unadj

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "./bt_result"


@dataclass(frozen=True)
class RtMarketFactorsFetcher:
    """Compute intraday factor snapshot using rt_stock_hist_unadj and cached weights."""

    engine: Engine
    output_dir: str = DEFAULT_OUTPUT_DIR

    def fetch_snapshot(self) -> RawBatch:
        """Compute one intraday factor snapshot row."""
        rt_df = self._load_rt_snapshot()
        if rt_df.empty:
            logger.warning("rt_stock_hist_unadj is empty")
            return []
        latest_time = rt_df["latest_time"].max()
        if not isinstance(latest_time, pd.Timestamp):
            latest_time = pd.to_datetime(latest_time, errors="coerce")
        if latest_time is pd.NaT:
            logger.warning("rt_stock_hist_unadj latest_time invalid")
            return []
        if latest_time.date() != date.today():
            logger.warning("rt_stock_hist_unadj has no data for today")
            return []

        returns = self._compute_rt_returns(rt_df)
        if returns.empty:
            logger.warning("rt_stock_hist_unadj returns empty")
            return []

        portfolio_returns = self._compute_portfolio_returns(returns)
        if portfolio_returns is None:
            return []

        snapshot = self._compute_factors(portfolio_returns, returns, latest_time.to_pydatetime())
        return [snapshot]

    def _load_rt_snapshot(self) -> pd.DataFrame:
        stmt = select(
            rt_stock_hist_unadj.c.stock_code,
            rt_stock_hist_unadj.c.close,
            rt_stock_hist_unadj.c.pre_close,
            rt_stock_hist_unadj.c.latest_time,
        )
        df = pd.read_sql(stmt, self.engine)
        if df.empty:
            return df
        df["latest_time"] = pd.to_datetime(df["latest_time"], errors="coerce")
        return df

    @staticmethod
    def _compute_rt_returns(df: pd.DataFrame) -> pd.Series:
        filtered = df.dropna(subset=["close", "pre_close"])
        filtered = filtered[filtered["pre_close"] != 0]
        if filtered.empty:
            return pd.Series(dtype="float64")
        series = (
            filtered.set_index("stock_code")["close"]
            / filtered.set_index("stock_code")["pre_close"]
            - 1.0
        )
        return series.dropna()

    def _compute_portfolio_returns(self, returns: pd.Series) -> dict[str, float] | None:
        returns_dict: dict[str, float] = {}
        for factor in ("bm", "qmj"):
            for size in ("S", "B"):
                for score in ("L", "M", "H"):
                    name = f"{factor}_{size}{score}"
                    weights = self._load_latest_weights(name)
                    if weights is None:
                        logger.warning("missing weights for %s", name)
                        return None
                    ret = _weighted_return(weights, returns)
                    if ret is None:
                        logger.warning("missing returns for %s", name)
                        return None
                    returns_dict[name] = ret
        return returns_dict

    def _load_latest_weights(self, name: str) -> pd.Series | None:
        path = os.path.join(self.output_dir, f"{name}_weights.csv")
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if df.empty:
            return None
        latest = df.iloc[-1]
        latest = latest.dropna()
        if latest.empty:
            return None
        return latest.astype("float64")

    @staticmethod
    def _compute_factors(
        returns_dict: dict[str, float],
        mkt_returns: pd.Series,
        latest_time: datetime,
    ) -> dict[str, object]:
        smb_bm = _mean(
            [
                returns_dict["bm_SL"],
                returns_dict["bm_SM"],
                returns_dict["bm_SH"],
            ]
        ) - _mean([returns_dict["bm_BL"], returns_dict["bm_BM"], returns_dict["bm_BH"]])
        smb_qmj = _mean(
            [
                returns_dict["qmj_SL"],
                returns_dict["qmj_SM"],
                returns_dict["qmj_SH"],
            ]
        ) - _mean([returns_dict["qmj_BL"], returns_dict["qmj_BM"], returns_dict["qmj_BH"]])
        smb = (smb_bm + smb_qmj) / 2
        hml = _mean([returns_dict["bm_SH"], returns_dict["bm_BH"]]) - _mean(
            [returns_dict["bm_SL"], returns_dict["bm_BL"]]
        )
        qmj = _mean([returns_dict["qmj_SH"], returns_dict["qmj_BH"]]) - _mean(
            [returns_dict["qmj_SL"], returns_dict["qmj_BL"]]
        )
        mkt = float(mkt_returns.mean()) if not mkt_returns.empty else None
        return {
            "latest_date": latest_time,
            "MKT": _safe_value(mkt),
            "SMB": _safe_value(smb),
            "HML": _safe_value(hml),
            "QMJ": _safe_value(qmj),
        }


def _weighted_return(weights: pd.Series, returns: pd.Series) -> float | None:
    aligned = weights[weights.index.isin(returns.index)]
    if aligned.empty:
        return None
    sum_weights = aligned.sum()
    if sum_weights == 0:
        return None
    values = returns[aligned.index]
    return float((aligned * values).sum() / sum_weights)


def _mean(values: list[float]) -> float:
    return float(sum(values) / len(values))


def _safe_value(value: float | None) -> float | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    return value
