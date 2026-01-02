from __future__ import annotations

import logging
from datetime import timedelta
from typing import cast

import pandas as pd
from sqlalchemy import Select, and_, select
from sqlalchemy.engine import Engine
from tqdm import tqdm

from infra.db.tables import (
    adj_factor,
    fundamental_data,
    index_hist,
    stock_hist_unadj,
    stock_info,
    trade_calendar,
)

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetch stock-level data for factor backtests."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_stock_info_df(self) -> pd.DataFrame:
        """Return stock info DataFrame indexed by stock_code."""
        stmt = select(
            stock_info.c.stock_code,
            stock_info.c.exchange,
            stock_info.c.listing_date,
            stock_info.c.list_status,
            stock_info.c.industry,
        )
        df = pd.read_sql(stmt, self.engine)
        df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")
        df = df.set_index("stock_code", drop=True)
        return df[["exchange", "listing_date", "list_status", "industry"]]

    def fetch_mkt_cap_on(self, stock_codes: list[str], date: str) -> pd.Series:
        """Fetch market cap series on a single date."""
        try:
            stmt = select(stock_hist_unadj.c.stock_code, stock_hist_unadj.c.mkt_cap).where(
                stock_hist_unadj.c.date == date
            )
            if stock_codes:
                stmt = stmt.where(stock_hist_unadj.c.stock_code.in_(stock_codes))
            df = pd.read_sql(stmt, self.engine)
        except Exception as exc:
            logger.error("Failed to fetch mkt_cap on %s: %s", date, exc)
            return pd.Series(dtype="float32")

        if df.empty:
            return pd.Series(dtype="float32")

        df = df.dropna(subset=["mkt_cap"])
        return df.set_index("stock_code")["mkt_cap"].astype("float32")

    def fetch_price(self, field: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch daily stock field values between start and end dates."""
        current = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        result = []

        chunks = []
        while current <= end:
            chunk_end = min(current + pd.DateOffset(days=30), end)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)

        for chunk_start, chunk_end in tqdm(chunks, desc=f"Fetching {field}", unit="chunk"):
            try:
                stmt = select(
                    stock_hist_unadj.c.date,
                    stock_hist_unadj.c.stock_code,
                    getattr(stock_hist_unadj.c, field),
                ).where(
                    and_(
                        stock_hist_unadj.c.date >= chunk_start.strftime("%Y-%m-%d"),
                        stock_hist_unadj.c.date <= chunk_end.strftime("%Y-%m-%d"),
                    )
                )
                df = pd.read_sql(stmt, self.engine)
                if not df.empty:
                    df = df.dropna(subset=[field])
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.pivot(index="date", columns="stock_code", values=field)
                    result.append(df.astype("float32"))
            except Exception as exc:
                logger.error(
                    "Failed to fetch %s from %s to %s: %s",
                    field,
                    chunk_start.date(),
                    chunk_end.date(),
                    exc,
                )

        if result:
            return pd.concat(result).sort_index()
        return pd.DataFrame()

    def fetch_adjfactor(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch adjustment factors between start and end dates."""
        current = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        result = []

        chunks = []
        while current <= end:
            chunk_end = min(current + pd.DateOffset(days=30), end)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)

        for chunk_start, chunk_end in tqdm(chunks, desc="Fetching adjusted factor", unit="chunk"):
            try:
                stmt = select(
                    adj_factor.c.date,
                    adj_factor.c.stock_code,
                    adj_factor.c.adj_factor,
                ).where(
                    and_(
                        adj_factor.c.date >= chunk_start.strftime("%Y-%m-%d"),
                        adj_factor.c.date <= chunk_end.strftime("%Y-%m-%d"),
                    )
                )
                df = pd.read_sql(stmt, self.engine)
                if not df.empty:
                    df = df.dropna()
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.pivot(index="date", columns="stock_code", values="adj_factor")
                    result.append(df.astype("float32"))
            except Exception as exc:
                logger.error(
                    "Failed to fetch adj_factor from %s to %s: %s",
                    chunk_start.date(),
                    chunk_end.date(),
                    exc,
                )

        if result:
            return pd.concat(result).sort_index().ffill()
        return pd.DataFrame()

    def fetch_adj_hist(self, field: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch adjusted historical price by applying adj_factor ratios."""
        pivot_df_hist = self.fetch_price(field, start_date, end_date)
        pivot_adjf = self.fetch_adjfactor(start_date, end_date)

        if set(pivot_df_hist.columns) != set(pivot_adjf.columns):
            logger.warning("Columns of price and adj_factor mismatch, intersecting them.")
        common_stocks = pivot_df_hist.columns.intersection(pivot_adjf.columns)
        pivot_df_hist = pivot_df_hist[common_stocks]
        pivot_adjf = pivot_adjf[common_stocks]

        pivot_adjf = pivot_adjf.reindex(pivot_df_hist.index)
        pivot_adjf = pivot_adjf.ffill()

        latest_adj_factors = pivot_adjf.iloc[-1]
        adj_ratios = pivot_adjf.div(latest_adj_factors, axis="columns")

        adjusted_prices = pivot_df_hist * adj_ratios
        adjusted_prices.index = pd.to_datetime(adjusted_prices.index)
        return adjusted_prices.astype("float32")

    def fetch_fundamentals_on(self, field: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch fundamentals for a single field in an adjusted window."""
        adj_start = pd.to_datetime(start_date) - pd.Timedelta(days=120)
        adj_end = pd.to_datetime(end_date) - pd.Timedelta(days=120)
        try:
            stmt = select(
                fundamental_data.c.report_date,
                fundamental_data.c.stock_code,
                getattr(fundamental_data.c, field),
            ).where(
                and_(
                    fundamental_data.c.report_date >= adj_start,
                    fundamental_data.c.report_date <= adj_end,
                )
            )
            df = pd.read_sql(stmt, self.engine)
        except Exception as exc:
            logger.error(
                "Failed to fetch fundamental %s from %s to %s: %s",
                field,
                start_date,
                end_date,
                exc,
            )
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        df = df.dropna()
        df["report_date"] = pd.to_datetime(df["report_date"])
        df = df.pivot(index="report_date", columns="stock_code", values=field)
        return df.astype("float32")

    def fetch_fundamentals_on_all(
        self, start_date: str, end_date: str, fields: list[str]
    ) -> pd.DataFrame:
        """Fetch multiple fundamental fields with a lookback window."""
        adj_start = pd.to_datetime(start_date) - pd.Timedelta(days=500)
        adj_end = pd.to_datetime(end_date) - pd.Timedelta(days=120)
        try:
            stmt = select(
                fundamental_data.c.report_date,
                fundamental_data.c.stock_code,
                *[getattr(fundamental_data.c, field) for field in fields],
            ).where(
                and_(
                    fundamental_data.c.report_date >= adj_start,
                    fundamental_data.c.report_date <= adj_end,
                )
            )
            df = pd.read_sql(stmt, self.engine)
        except Exception as exc:
            logger.error(
                "Failed to fetch fundamentals from %s to %s: %s",
                start_date,
                end_date,
                exc,
            )
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        df = df.dropna(subset=["stock_code", "report_date"])
        df["report_date"] = pd.to_datetime(df["report_date"])
        df = df.set_index(["report_date", "stock_code"])
        return df.astype("float32")


class CSIIndexDataFetcher:
    """Fetch index data from index_hist table."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_data_by_code_and_date(
        self, code: str, start: str, end: str, fields: list[str]
    ) -> pd.DataFrame:
        """Return index data for a code and date range."""
        cols = [index_hist.c.date]
        for field in fields:
            if field != "date":
                cols.append(getattr(index_hist.c, field))
        stmt: Select = select(*cols).where(
            and_(
                index_hist.c.index_code == code,
                index_hist.c.date >= start,
                index_hist.c.date <= end,
            )
        )
        df = pd.read_sql(stmt, self.engine)
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"])
        return df


class CalendarFetcher:
    """Fetch trade calendar information."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_trade_date(self, start: str, end: str, format: str = "%Y-%m-%d") -> list[str]:
        """Return trade dates between start/end (YYYYMMDD) inclusive."""
        start_dt = pd.to_datetime(start, format="%Y%m%d")
        end_dt = pd.to_datetime(end, format="%Y%m%d")
        stmt = select(trade_calendar.c.date).where(
            and_(trade_calendar.c.date >= start_dt, trade_calendar.c.date <= end_dt)
        )
        df = pd.read_sql(stmt, self.engine)
        if df.empty:
            return []
        return [pd.to_datetime(value).strftime(format) for value in df["date"].sort_values()]

    def get_prev_trade_date(self, current_date: str, format: str = "%Y-%m-%d") -> str:
        """Return previous trade date before current_date."""
        current_dt = pd.to_datetime(current_date, format="%Y%m%d")
        stmt = select(trade_calendar.c.date).where(trade_calendar.c.date < current_dt)
        df = pd.read_sql(stmt, self.engine)
        if df.empty:
            raise ValueError("no trade dates before current_date")
        prev_date = pd.to_datetime(df["date"].max())
        return cast(str, prev_date.strftime(format))
