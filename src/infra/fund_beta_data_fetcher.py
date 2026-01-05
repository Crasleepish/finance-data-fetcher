from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd
from sqlalchemy import and_, select
from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from infra.db.tables import fund_beta, fund_hist, fund_info, market_factors

logger = logging.getLogger(__name__)

FACTOR_NAMES = ["MKT", "SMB", "HML", "QMJ"]


@dataclass(frozen=True)
class FundBetaDataFetcher:
    """Fetch fund beta inputs from the database."""

    engine: Engine
    calendar: TradingCalendarService

    def load_fund_codes(self, invest_types: Iterable[str]) -> list[str]:
        """Load fund codes matching the provided invest types."""
        stmt = select(fund_info.c.fund_code).where(fund_info.c.invest_type.in_(list(invest_types)))
        df = pd.read_sql(stmt, self.engine)
        codes = sorted({code for code in df["fund_code"].dropna().tolist() if code})
        return codes

    def get_market_factors(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return market factor DataFrame indexed by date."""
        stmt = (
            select(
                market_factors.c.date,
                market_factors.c.MKT,
                market_factors.c.SMB,
                market_factors.c.HML,
                market_factors.c.QMJ,
            )
            .where(market_factors.c.date >= start_date, market_factors.c.date <= end_date)
            .order_by(market_factors.c.date)
        )
        df = pd.read_sql(stmt, self.engine)
        if df.empty:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date", drop=True)
        return df[FACTOR_NAMES]

    def get_fund_daily_return(self, fund_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return fund daily returns indexed by date with forward-filled net value."""
        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        prev_date = self.calendar.prev_trade_day(start)
        query_start = prev_date or start

        stmt = (
            select(fund_hist.c.date, fund_hist.c.net_value)
            .where(
                and_(
                    fund_hist.c.fund_code == fund_code,
                    fund_hist.c.date >= query_start,
                    fund_hist.c.date <= end,
                )
            )
            .order_by(fund_hist.c.date)
        )
        df = pd.read_sql(stmt, self.engine)
        if df.empty:
            raise RuntimeError("No net value data")

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date", drop=True)
        net_series = df["net_value"].astype(float)

        trade_days = self.get_trade_days(start_date, end_date)
        if prev_date:
            trade_days = [prev_date] + trade_days
        if not trade_days:
            raise RuntimeError("No trade days for fund return range")
        trade_index = pd.to_datetime(trade_days)

        net_series = net_series.reindex(trade_index).ffill()
        if net_series.isna().all():
            raise RuntimeError("No net value data")

        if pd.isna(net_series.iloc[0]):
            net_series.iloc[0] = 1.0
        net_series = net_series.ffill()

        prev_values = net_series.shift(1)
        if pd.isna(prev_values.iloc[0]):
            prev_values.iloc[0] = 1.0
        prev_values = prev_values.ffill()

        daily_return = net_series / prev_values - 1.0
        daily_return = daily_return.iloc[1:] if prev_date else daily_return
        result = pd.DataFrame({"daily_return": daily_return})
        result = result.sort_index()
        if result["daily_return"].isna().any():
            raise RuntimeError("daily_return contains NaN after fill")
        return result

    def get_trade_days(self, start_date: str, end_date: str) -> list[date]:
        """Return trade days within the date range."""
        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=10000)
        return [day for chunk in chunks for day in chunk]

    def prev_trade_day(self, day: date) -> date | None:
        """Return previous trade day for a given date."""
        return self.calendar.prev_trade_day(day)

    def get_beta_by_code_date(self, fund_code: str, day: date) -> pd.DataFrame:
        """Return beta records for a fund and date."""
        stmt = select(fund_beta).where(
            fund_beta.c.code == fund_code,
            fund_beta.c.date == day,
        )
        return pd.read_sql(stmt, self.engine)

    def get_latest_beta(self, fund_code: str) -> pd.DataFrame:
        """Return the latest beta record for a fund."""
        stmt = (
            select(fund_beta)
            .where(fund_beta.c.code == fund_code)
            .order_by(fund_beta.c.date.desc())
            .limit(1)
        )
        return pd.read_sql(stmt, self.engine)

    def get_all_beta(self, fund_code: str) -> pd.DataFrame:
        """Return all beta records for a fund."""
        stmt = select(fund_beta).where(fund_beta.c.code == fund_code)
        return pd.read_sql(stmt, self.engine)
