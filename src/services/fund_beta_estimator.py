from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, cast

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from core.beta.covariance import covariance_to_json, pack_covariance, safe_value
from core.beta.kalman_filter import KalmanFilter
from core.beta.q_r_estimator import QREstimator
from core.pipeline.types import RawBatch
from infra.fund_beta_data_fetcher import FACTOR_NAMES, FundBetaDataFetcher

logger = logging.getLogger(__name__)

WINDOW_SIZE = 60
ALPHA_RHO = 0.98
GAMMA_RHO = 0.99
Q_GAMMA = 1e-5
USE_JOSEPH = True
USE_ECM = True
ECM_META_JSON = '{"dtype": "float32", "n": 6}'


@dataclass(frozen=True)
class FundBetaEstimator:
    """Compute fund beta exposures using Kalman + ECM + QR."""

    data_fetcher: FundBetaDataFetcher

    def run_historical_beta(self, fund_code: str, start_date: str, end_date: str) -> RawBatch:
        """Run historical beta estimation for a fund."""
        factor_df = self.data_fetcher.get_market_factors(start_date, end_date)
        return_df = self.data_fetcher.get_fund_daily_return(fund_code, start_date, end_date).dropna(
            how="any"
        )
        if len(return_df) == 0:
            raise RuntimeError("No daily return data")

        factor_df.index = pd.to_datetime(factor_df.index)
        return_df.index = pd.to_datetime(return_df.index)

        df = factor_df.join(return_df).sort_index().dropna(how="any")
        df["intercept"] = 1.0

        qr = QREstimator(window_size=WINDOW_SIZE)

        init_df = df.iloc[:WINDOW_SIZE]
        X_ols = init_df[FACTOR_NAMES + ["intercept"]].values
        y_ols = init_df["daily_return"].values
        model = LinearRegression().fit(X_ols, y_ols)
        z0 = model.coef_.reshape(-1, 1)
        P0 = np.diag([1.0, 1.0, 1.0, 1.0, 0.1])

        kf = KalmanFilter(
            state_dim=5,
            z0=z0,
            P0=P0,
            alpha_index=-1,
            alpha_rho=ALPHA_RHO,
            use_joseph=USE_JOSEPH,
            use_ecm=USE_ECM,
            gamma_rho=GAMMA_RHO,
            q_gamma=Q_GAMMA,
        )

        log_nav_true = 0.0
        log_nav_fit = 0.0
        records: list[dict[str, object]] = []

        for idx_date, row in df.iterrows():
            x = np.array([row[f] for f in FACTOR_NAMES] + [1.0], dtype=float)
            y = float(row["daily_return"])

            z_prev = kf.current_state()
            y_fit_pred = float(x @ z_prev[:5, :].ravel())
            y_fit_pred = np.clip(y_fit_pred, -0.999999, None)

            te_prev = float(log_nav_true - log_nav_fit)

            qr.update_data(np.array([x]), y)
            Q, R = qr.estimate()

            z = kf.step(H=x.reshape(-1, 1), y=y, Q=Q, R=R, te_prev=te_prev)

            log_nav_true += np.log1p(np.clip(y, -0.999999, None))
            log_nav_fit += np.log1p(y_fit_pred)

            z_all = z.flatten().tolist()
            z_plain = z_all[:5]
            gamma_val = z_all[5]

            beta_dict = dict(zip(FACTOR_NAMES + ["const"], (safe_value(v) for v in z_plain)))
            beta_dict["gamma"] = safe_value(gamma_val)

            records.append(
                {
                    "code": fund_code,
                    "date": idx_date.date(),
                    "MKT": beta_dict["MKT"],
                    "SMB": beta_dict["SMB"],
                    "HML": beta_dict["HML"],
                    "QMJ": beta_dict["QMJ"],
                    "const": beta_dict["const"],
                    "gamma": beta_dict["gamma"],
                    "P_json": covariance_to_json(kf.current_cov()),
                    "P_bin": pack_covariance(kf.current_cov()),
                    "log_nav_true": float(log_nav_true),
                    "log_nav_fit": float(log_nav_fit),
                }
            )

        return records

    def run_realtime_update(
        self,
        fund_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        fallback_to_full: bool = True,
    ) -> RawBatch:
        """Run incremental beta updates for a fund."""
        latest = None
        if start_date:
            pre_date = self._prev_trade_date(start_date)
            latest_df = (
                self.data_fetcher.get_beta_by_code_date(fund_code, pre_date)
                if pre_date
                else pd.DataFrame()
            )
            if not latest_df.empty:
                latest = latest_df.iloc[0]
                target_columns = [
                    "MKT",
                    "SMB",
                    "HML",
                    "QMJ",
                    "P_bin",
                    "log_nav_fit",
                    "log_nav_true",
                    "gamma",
                ]
                if latest[target_columns].isna().any():
                    latest = None
                    all_df = self.data_fetcher.get_all_beta(fund_code)
                    valid_mask = all_df[target_columns].notna().all(axis=1)
                    valid_records = all_df[valid_mask]
                    if not valid_records.empty:
                        latest = valid_records.sort_values("date", ascending=False).iloc[0]
                        if (pd.to_datetime(start_date) - pd.to_datetime(latest["date"])).days > 5:
                            logger.warning(
                                "latest valid beta too old for %s; skipping",
                                fund_code,
                            )
                            return []
            elif not fallback_to_full:
                raise ValueError("Missing historical beta before start_date")
        else:
            latest_df = self.data_fetcher.get_latest_beta(fund_code)
            if not latest_df.empty:
                latest = latest_df.iloc[0]
            elif not fallback_to_full:
                raise ValueError("Missing historical beta for realtime update")

        if latest is None:
            hist_start = self._earliest_common_date(
                fund_code, end_date or pd.Timestamp.today().strftime("%Y-%m-%d")
            )
            hist_end = end_date or pd.Timestamp.today().strftime("%Y-%m-%d")
            return self.run_historical_beta(fund_code, hist_start, hist_end)

        z_prev = np.array(
            [latest.MKT, latest.SMB, latest.HML, latest.QMJ, latest.const, latest.gamma],
            dtype=float,
        ).reshape(-1, 1)

        if pd.notna(latest.P_bin):
            from core.beta.covariance import unpack_covariance

            P_prev = unpack_covariance(latest.P_bin, ECM_META_JSON)
        else:
            P_prev = np.diag([1.0, 1.0, 1.0, 1.0, 0.1])

        if pd.isna(latest.log_nav_true) or pd.isna(latest.log_nav_fit):
            if not fallback_to_full:
                raise ValueError("Missing log_nav values for realtime update")
            hist_start = self._earliest_common_date(
                fund_code,
                pd.to_datetime(latest.date).strftime("%Y-%m-%d"),
            )
            self.run_historical_beta(
                fund_code,
                hist_start,
                pd.to_datetime(latest.date).strftime("%Y-%m-%d"),
            )
            latest_df = self.data_fetcher.get_latest_beta(fund_code)
            if latest_df.empty:
                raise RuntimeError("Failed to rebuild history for realtime update")
            latest = latest_df.iloc[0]
            z_prev = np.array(
                [latest.MKT, latest.SMB, latest.HML, latest.QMJ, latest.const, latest.gamma],
                dtype=float,
            ).reshape(-1, 1)
            if pd.notna(latest.P_bin):
                from core.beta.covariance import unpack_covariance

                P_prev = unpack_covariance(latest.P_bin, ECM_META_JSON)

        log_nav_true = float(latest.log_nav_true)
        log_nav_fit = float(latest.log_nav_fit)

        start_date = (pd.to_datetime(latest.date) + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = end_date or pd.Timestamp.today().strftime("%Y-%m-%d")

        qr = self._bootstrap_qr_from_history(fund_code, start_date)

        factor_df = self.data_fetcher.get_market_factors(start_date, end_date)
        ret_df = self.data_fetcher.get_fund_daily_return(fund_code, start_date, end_date)
        df = factor_df.join(ret_df).sort_index()
        if df.empty:
            return []
        df["intercept"] = 1.0

        kf = KalmanFilter(
            state_dim=5,
            z0=z_prev,
            P0=P_prev,
            alpha_index=-1,
            alpha_rho=ALPHA_RHO,
            use_joseph=USE_JOSEPH,
            use_ecm=USE_ECM,
            gamma_rho=GAMMA_RHO,
            q_gamma=Q_GAMMA,
        )

        records: list[dict[str, object]] = []
        for idx_date, row in df.iterrows():
            x = np.array([row[f] for f in FACTOR_NAMES] + [1.0], dtype=float)
            y = float(row["daily_return"])

            z_prev_full = kf.current_state()
            y_fit_pred = float(x @ z_prev_full[:5, :].ravel())
            y_fit_pred = np.clip(y_fit_pred, -0.999999, None)

            te_prev = float(log_nav_true - log_nav_fit)

            qr.update_data(np.array([x]), y)
            Q, R = qr.estimate()
            z = kf.step(H=x.reshape(-1, 1), y=y, Q=Q, R=R, te_prev=te_prev)

            log_nav_true += np.log1p(np.clip(y, -0.999999, None))
            log_nav_fit += np.log1p(y_fit_pred)

            z_all = z.flatten().tolist()
            z_plain = z_all[:5]
            gamma_val = z_all[5]

            beta_dict = dict(zip(FACTOR_NAMES + ["const"], (safe_value(v) for v in z_plain)))
            beta_dict["gamma"] = safe_value(gamma_val)

            records.append(
                {
                    "code": fund_code,
                    "date": idx_date.date(),
                    "MKT": beta_dict["MKT"],
                    "SMB": beta_dict["SMB"],
                    "HML": beta_dict["HML"],
                    "QMJ": beta_dict["QMJ"],
                    "const": beta_dict["const"],
                    "gamma": beta_dict["gamma"],
                    "P_json": covariance_to_json(kf.current_cov()),
                    "P_bin": pack_covariance(kf.current_cov()),
                    "log_nav_true": float(log_nav_true),
                    "log_nav_fit": float(log_nav_fit),
                }
            )

        return records

    def run_realtime_batch(
        self,
        fund_codes: Iterable[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> RawBatch:
        """Run realtime updates across funds and aggregate records."""
        records: list[dict[str, object]] = []
        for code in fund_codes:
            try:
                batch = [
                    dict(item) for item in self.run_realtime_update(code, start_date, end_date)
                ]
                records.extend(batch)
            except Exception as exc:
                logger.error("fund %s beta update failed: %s", code, exc)
        return records

    def _earliest_common_date(self, fund_code: str, end_date: str) -> str:
        start_probe = "1990-01-01"
        factor_df = self.data_fetcher.get_market_factors(start_probe, end_date)
        ret_df = self.data_fetcher.get_fund_daily_return(fund_code, start_probe, end_date)

        factor_df.index = pd.to_datetime(factor_df.index)
        ret_df.index = pd.to_datetime(ret_df.index)
        df = factor_df.join(ret_df).dropna(how="any").sort_index()
        if df.empty:
            raise RuntimeError("无法找到可用的共同历史数据（因子或收益缺失）。")
        first_date = pd.to_datetime(df.index[0])
        return cast(str, first_date.strftime("%Y-%m-%d"))

    def _bootstrap_qr_from_history(self, fund_code: str, ref_date: str) -> QREstimator:
        trade_dates = self._trade_dates_before(ref_date, 2 * WINDOW_SIZE + 1)
        if not trade_dates:
            return QREstimator(window_size=WINDOW_SIZE)

        trade_dates.sort()
        start_hist = trade_dates[0]
        end_hist = trade_dates[-1]

        factor_df = self.data_fetcher.get_market_factors(start_hist, end_hist)
        ret_df = self.data_fetcher.get_fund_daily_return(fund_code, start_hist, end_hist)

        df_hist = factor_df.join(ret_df).sort_index()
        df_hist = df_hist.dropna(how="any")
        if df_hist.empty:
            return QREstimator(window_size=WINDOW_SIZE)

        df_hist["intercept"] = 1.0
        X_all = df_hist[FACTOR_NAMES + ["intercept"]].values.astype(float)
        y_all = df_hist["daily_return"].values.astype(float)
        X_win = X_all[-2 * WINDOW_SIZE :, :]
        y_win = y_all[-2 * WINDOW_SIZE :]

        qr = QREstimator(window_size=WINDOW_SIZE, base_dim=5)
        for i in range(len(X_win)):
            qr.update_data(X_win[i : i + 1, :], float(y_win[i]))
            qr.estimate()
        return qr

    def _trade_dates_before(self, ref_date: str, limit: int) -> list[str]:
        start = "1990-01-01"
        trade_days = self.data_fetcher.get_trade_days(start, ref_date)
        ref = pd.to_datetime(ref_date)
        filtered = [d for d in trade_days if pd.to_datetime(d) < ref]
        if not filtered:
            return []
        filtered = filtered[-limit:]
        formatted: list[str] = []
        for day in filtered:
            timestamp = pd.to_datetime(day)
            formatted.append(cast(str, timestamp.strftime("%Y-%m-%d")))
        return formatted

    def _prev_trade_date(self, start_date: str) -> date | None:
        return self.data_fetcher.prev_trade_day(pd.to_datetime(start_date).date())
