from __future__ import annotations

import logging
from dataclasses import dataclass, field
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
    _factor_cache: dict[tuple[str, str], pd.DataFrame] = field(
        default_factory=dict, init=False, repr=False
    )
    _trade_days_cache: dict[tuple[str, str], list[date]] = field(
        default_factory=dict, init=False, repr=False
    )
    _fund_net_cache: dict[tuple[str, str], dict[str, pd.Series]] = field(
        default_factory=dict, init=False, repr=False
    )
    _fund_query_start_cache: dict[tuple[str, str], date] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "_factor_cache", {})
        object.__setattr__(self, "_trade_days_cache", {})
        object.__setattr__(self, "_fund_net_cache", {})
        object.__setattr__(self, "_fund_query_start_cache", {})

    def prime_fund_net_values(
        self, fund_codes: Iterable[str], start_date: str, end_date: str
    ) -> None:
        """Preload net_value series for a date range to reduce per-fund queries."""
        cache_key = (start_date, end_date)
        if cache_key in self._fund_net_cache:
            return

        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        prev_date = self.calendar.prev_trade_day(start)
        query_start = prev_date or start
        codes = list({code for code in fund_codes if code})
        if not codes:
            self._fund_net_cache[cache_key] = {}
            self._fund_query_start_cache[cache_key] = query_start
            return

        stmt = (
            select(fund_hist.c.fund_code, fund_hist.c.date, fund_hist.c.net_value)
            .where(
                and_(
                    fund_hist.c.fund_code.in_(codes),
                    fund_hist.c.date >= query_start,
                    fund_hist.c.date <= end,
                )
            )
            .order_by(fund_hist.c.fund_code, fund_hist.c.date)
        )
        df = pd.read_sql(stmt, self.engine)
        df["date"] = pd.to_datetime(df["date"])
        cache: dict[str, pd.Series] = {}
        for fund_code, group in df.groupby("fund_code"):
            series = group.set_index("date")["net_value"].astype(float)
            cache[fund_code] = series
        self._fund_net_cache[cache_key] = cache
        self._fund_query_start_cache[cache_key] = query_start

    def load_fund_codes(self, invest_types: Iterable[str]) -> list[str]:
        """Load fund codes matching the provided invest types."""
        stmt = select(fund_info.c.fund_code).where(fund_info.c.invest_type.in_(list(invest_types)))
        df = pd.read_sql(stmt, self.engine)
        codes = sorted({code for code in df["fund_code"].dropna().tolist() if code})
        return codes

    def load_fund_codes_with_data(
        self, invest_types: Iterable[str], start_date: str, end_date: str
    ) -> list[tuple[str, str]]:
        """Load fund codes with net_value data and per-fund computation start."""
        end = pd.to_datetime(end_date).date()
        min_listed_date = (pd.to_datetime(end_date) - pd.DateOffset(years=1)).date()
        start = pd.to_datetime(start_date).date()

        stmt = select(fund_info.c.fund_code, fund_info.c.found_date).where(
            fund_info.c.invest_type.in_(list(invest_types)),
            fund_info.c.found_date.is_not(None),
            fund_info.c.found_date <= min_listed_date,
        )
        fund_df = pd.read_sql(stmt, self.engine)
        if fund_df.empty:
            return []

        fund_df = fund_df.dropna(subset=["fund_code", "found_date"])
        fund_df["found_date"] = pd.to_datetime(fund_df["found_date"]).dt.date
        fund_df["fund_start"] = (
            pd.to_datetime(fund_df["found_date"]) + pd.DateOffset(years=1)
        ).dt.date
        fund_df["fund_start"] = fund_df["fund_start"].apply(
            lambda value: value if value > start else start
        )

        min_start = fund_df["fund_start"].min()
        if min_start is None:
            return []

        hist_stmt = (
            select(fund_hist.c.fund_code, fund_hist.c.date)
            .where(
                fund_hist.c.fund_code.in_(fund_df["fund_code"].tolist()),
                fund_hist.c.date >= min_start,
                fund_hist.c.date <= end,
                fund_hist.c.net_value.is_not(None),
            )
            .order_by(fund_hist.c.fund_code, fund_hist.c.date)
        )
        hist_df = pd.read_sql(hist_stmt, self.engine)
        if hist_df.empty:
            return []

        hist_df["date"] = pd.to_datetime(hist_df["date"])
        max_dates = hist_df.groupby("fund_code")["date"].max()
        fund_df = fund_df.set_index("fund_code")
        fund_df["max_date"] = max_dates
        fund_df = fund_df.dropna(subset=["max_date"])
        eligible = fund_df[fund_df["max_date"].dt.date >= fund_df["fund_start"]]
        if eligible.empty:
            return []

        result = [
            (code, start_value.strftime("%Y-%m-%d"))
            for code, start_value in eligible["fund_start"].items()
        ]
        result.sort(key=lambda item: item[0])
        return result

    def get_market_factors(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return market factor DataFrame indexed by date."""
        cache_key = (start_date, end_date)
        cached = self._factor_cache.get(cache_key)
        if cached is not None:
            return cached
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
        result = df[FACTOR_NAMES]
        self._factor_cache[cache_key] = result
        return result

    def get_fund_daily_return(self, fund_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return fund daily returns indexed by date with forward-filled net value."""
        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        cache_key = (start_date, end_date)
        cached = self._fund_net_cache.get(cache_key)
        if cached is not None and fund_code in cached:
            net_series = cached[fund_code]
            query_start = self._fund_query_start_cache.get(cache_key, start)
        else:
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
        if query_start < start:
            trade_days = [query_start] + trade_days
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
        daily_return = daily_return.iloc[1:] if query_start < start else daily_return
        result = pd.DataFrame({"daily_return": daily_return})
        result = result.sort_index()
        if result["daily_return"].isna().any():
            raise RuntimeError("daily_return contains NaN after fill")
        return result

    def get_trade_days(self, start_date: str, end_date: str) -> list[date]:
        """Return trade days within the date range."""
        cache_key = (start_date, end_date)
        cached = self._trade_days_cache.get(cache_key)
        if cached is not None:
            return cached
        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        chunks = self.calendar.normalize_trade_day_chunks(start, end, chunk_size=10000)
        days = [day for chunk in chunks for day in chunk]
        self._trade_days_cache[cache_key] = days
        return days

    def get_bootstrap_range(self, ref_date: str, window_size: int) -> tuple[str, str] | None:
        """Return the date range needed to bootstrap QR estimation."""
        trade_days = self.get_trade_days("1990-01-01", ref_date)
        ref = pd.to_datetime(ref_date).date()
        filtered = [day for day in trade_days if day < ref]
        if not filtered:
            return None
        limit = 2 * window_size + 1
        window = filtered[-limit:] if len(filtered) >= limit else filtered
        start_hist = window[0].strftime("%Y-%m-%d")
        end_hist = window[-1].strftime("%Y-%m-%d")
        return start_hist, end_hist

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
