from __future__ import annotations

import logging
import os
from typing import Callable

import pandas as pd
from sqlalchemy.engine import Engine

from core.pipeline.types import RawBatch
from infra.factor_data_fetcher import CSIIndexDataFetcher
from services.portfolio_driver import build_all_portfolios

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = "./bt_result"


def _safe_value(value: object) -> object:
    """Return None for NaN-like values to match DB-null semantics."""
    return None if pd.isna(value) else value


class FactorFetcher:
    """Orchestrate factor backtests and aggregate daily factor returns."""

    def __init__(self, engine: Engine, output_dir: str = DEFAULT_OUTPUT_DIR) -> None:
        self.engine = engine
        self.output_dir = output_dir

    def fetch_all(
        self,
        start_date: str,
        end_date: str,
        mode: str,
        progress_callback: Callable[[float, float], None] | None = None,
    ) -> RawBatch:
        """Run portfolio backtests and return daily factor records."""
        logger.info("Starting factor fetch from %s to %s", start_date, end_date)
        build_all_portfolios(start_date, end_date, mode, self.engine)
        factors_df = self._compute_daily_factors(start_date, end_date)

        if progress_callback:
            progress_callback(100, 100)

        return self._to_raw_batch(factors_df)

    def _compute_daily_factors(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Compute MKT/SMB/HML/QMJ factor series from portfolio returns."""
        logger.info("Reading backtest output: %s", self.output_dir)
        returns_dict: dict[str, pd.DataFrame] = {}

        returns_dict.update(self._load_portfolio_returns("bm"))
        returns_dict.update(self._load_portfolio_returns("qmj"))

        if not returns_dict:
            logger.warning("No portfolio returns loaded from %s", self.output_dir)
            return pd.DataFrame()

        merged_df = pd.concat(returns_dict.values(), axis=1).dropna()
        merged_df = merged_df.sort_index()
        merged_df.index.name = "date"

        index_df = CSIIndexDataFetcher(self.engine).get_data_by_code_and_date(
            "000985.CSI", start=start_date, end=end_date, fields=["date", "change_percent"]
        )
        if index_df.empty:
            message = f"No index data found for MKT between {start_date} and {end_date}"
            logger.error(message)
            raise ValueError(message)
        else:
            index_df = index_df.rename(columns={"change_percent": "MKT"})
            index_df = index_df.set_index("date", drop=True)
            index_df["MKT"] = index_df["MKT"] / 100.0
            index_df = index_df.reindex(merged_df.index)
            index_df["MKT"] = index_df["MKT"].fillna(0.0)

        merged_df = merged_df.join(index_df, how="left")
        if merged_df.empty:
            logger.warning("No overlapping dates after joining index returns.")
            return pd.DataFrame()

        smb_bm = merged_df[["bm_SL", "bm_SM", "bm_SH"]].mean(axis=1) - merged_df[
            ["bm_BL", "bm_BM", "bm_BH"]
        ].mean(axis=1)
        smb_qmj = merged_df[["qmj_SL", "qmj_SM", "qmj_SH"]].mean(axis=1) - merged_df[
            ["qmj_BL", "qmj_BM", "qmj_BH"]
        ].mean(axis=1)
        smb = (smb_bm + smb_qmj) / 2

        hml = (merged_df["bm_SH"] + merged_df["bm_BH"]) / 2 - (
            merged_df["bm_SL"] + merged_df["bm_BL"]
        ) / 2

        qmj = (merged_df["qmj_SH"] + merged_df["qmj_BH"]) / 2 - (
            merged_df["qmj_SL"] + merged_df["qmj_BL"]
        ) / 2

        factors_df = pd.DataFrame(
            {
                "MKT": merged_df["MKT"],
                "SMB": smb,
                "HML": hml,
                "QMJ": qmj,
            },
            index=merged_df.index,
        )

        logger.info("Factor computation finished: %d rows", len(factors_df))
        return factors_df

    def _load_portfolio_returns(self, factor: str) -> dict[str, pd.DataFrame]:
        """Load portfolio return CSVs for a factor prefix."""
        returns_dict: dict[str, pd.DataFrame] = {}
        for size in ["S", "B"]:
            for score in ["L", "M", "H"]:
                file_name = f"{factor}_{size}{score}_daily_returns.csv"
                path = os.path.join(self.output_dir, file_name)
                if not os.path.exists(path):
                    logger.warning("Missing portfolio returns: %s", file_name)
                    continue
                df = pd.read_csv(path, parse_dates=["date"])[["date", "value"]]
                col_name = f"{factor}_{size}{score}"
                df = df.rename(columns={"value": col_name}).set_index("date")
                returns_dict[col_name] = df
        return returns_dict

    def _to_raw_batch(self, factors_df: pd.DataFrame) -> RawBatch:
        """Convert factor DataFrame to a RawBatch list of dicts."""
        if factors_df.empty:
            return []

        records: list[dict[str, object]] = []
        for date, row in factors_df.iterrows():
            records.append(
                {
                    "date": date.date(),
                    "MKT": _safe_value(row["MKT"]),
                    "SMB": _safe_value(row["SMB"]),
                    "HML": _safe_value(row["HML"]),
                    "QMJ": _safe_value(row["QMJ"]),
                    "VOL": None,
                    "LIQ": None,
                }
            )
        return records
