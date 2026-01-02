from __future__ import annotations

import logging
from typing import List, Optional, Protocol

import pandas as pd

logger = logging.getLogger(__name__)


class CalendarProvider(Protocol):
    """Protocol for fetching trade dates."""

    def get_trade_date(self, start: str, end: str, format: str = "%Y-%m-%d") -> list[str]:
        """Return trade dates between start and end."""

    def get_prev_trade_date(self, current_date: str, format: str = "%Y-%m-%d") -> str:
        """Return previous trade date before current_date."""


class RebalanceDateGenerator:
    """Generate rebalance dates based on frequency and calendar."""

    def __init__(
        self,
        freq: str,
        calendar_fetcher: CalendarProvider,
        anchor: str = "start",
        custom_months: Optional[List[int]] = None,
    ) -> None:
        self.freq = freq
        self.anchor = anchor
        self.custom_months = custom_months
        self.calendar_fetcher = calendar_fetcher

    def get_dates_from_range(self, start_date: str, end_date: str) -> pd.DatetimeIndex:
        """Return rebalance dates within [start_date, end_date]."""
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        def _trade_dates(d1: pd.Timestamp, d2: pd.Timestamp) -> pd.DatetimeIndex:
            ts = self.calendar_fetcher.get_trade_date(
                start=d1.strftime("%Y%m%d"),
                end=d2.strftime("%Y%m%d"),
                format="%Y-%m-%d",
            )
            return pd.to_datetime(ts)

        if self.freq == "daily":
            return _trade_dates(start, end)

        dates: list[pd.Timestamp] = []
        if self.freq in ("monthly", "custom_months"):
            months = pd.period_range(start.to_period("M"), end.to_period("M"), freq="M")
            if self.freq == "custom_months":
                months = [p for p in months if p.month in (self.custom_months or [])]

            for period in months:
                m_start = pd.Timestamp(period.start_time.date())
                m_end = (period + 1).start_time - pd.Timedelta(days=1)
                td = _trade_dates(m_start, m_end)
                if len(td) == 0:
                    continue
                selected = td[0] if self.anchor == "start" else td[-1]
                dates.append(selected)
        elif self.freq == "yearly":
            years = range(start.year, end.year + 1)
            for year in years:
                y_start = pd.Timestamp(f"{year}-01-01")
                y_end = pd.Timestamp(f"{year}-12-31")
                td = _trade_dates(y_start, y_end)
                if len(td) == 0:
                    continue
                selected = td[0] if self.anchor == "start" else td[-1]
                dates.append(selected)
        else:
            raise ValueError("Unsupported frequency")

        dates = [d for d in dates if (d >= start and d <= end)]
        return pd.DatetimeIndex(sorted(set(dates)))

    def get_prev_balance_date(self, current_date: str) -> Optional[pd.Timestamp]:
        start = (pd.to_datetime(current_date) - pd.Timedelta(days=366)).strftime("%Y-%m-%d")
        trade_dates_str = self.calendar_fetcher.get_trade_date(
            start=start.replace("-", ""),
            end=current_date.replace("-", ""),
            format="%Y-%m-%d",
        )
        trade_dates = pd.to_datetime(trade_dates_str)
        rebalance_dates = self.get_dates_from_range(
            trade_dates[0].strftime("%Y-%m-%d"),
            current_date,
        )
        rebalance_dates = rebalance_dates[rebalance_dates < pd.to_datetime(current_date)]
        if len(rebalance_dates) == 0:
            return None
        return rebalance_dates.max()
