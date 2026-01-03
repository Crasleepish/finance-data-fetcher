from __future__ import annotations

import logging
import math
from abc import ABC, abstractmethod
from typing import List, Optional, Set, cast

import pandas as pd

logger = logging.getLogger(__name__)


def get_latest_available_row(
    df: pd.DataFrame,
    asof_date: pd.Timestamp,
    check_fields: List[str] | None = None,
) -> pd.DataFrame:
    """Return latest report row within one year before asof_date."""
    one_year_ago = asof_date - pd.DateOffset(years=1)
    mask_date = (df.index.get_level_values("report_date") <= asof_date) & (
        df.index.get_level_values("report_date") >= one_year_ago
    )
    df = df.loc[mask_date]
    if df.empty:
        return pd.DataFrame()

    if check_fields:
        df = df.dropna(subset=check_fields, how="any")
        if df.empty:
            return pd.DataFrame()

    df = df.sort_index(level="report_date")
    latest = df.groupby(level="stock_code").tail(1)
    latest.index = latest.index.get_level_values("stock_code")
    return latest


class Selector(ABC):
    """Base selector with parent chain."""

    def __init__(self, parents: Optional[List["Selector"]] = None) -> None:
        self.parents = parents or []

    def select(self, universe: Optional[Set[str]] = None) -> List[str]:
        for parent in self.parents:
            universe = set(parent.select(universe))
            if not universe:
                return []
        return self._select(universe)

    @abstractmethod
    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        """Select stocks from the universe."""


class AmountSelector(Selector):
    """Filter stocks by removing the lowest percentile in amount."""

    def __init__(
        self,
        amount: pd.DataFrame,
        asof_date: pd.Timestamp,
        percentile: float,
        parents: List[Selector] | None = None,
    ) -> None:
        super().__init__(parents)
        self.amount = amount
        self.asof_date = asof_date
        self.percentile = percentile

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        if self.amount is None or self.asof_date not in self.amount.index:
            return []
        amount_series = self.amount.loc[self.asof_date].dropna()
        if universe is not None:
            amount_series = amount_series[amount_series.index.isin(universe)]
        if amount_series.empty:
            return []
        amount_series = amount_series.sort_values(ascending=True)
        n = len(amount_series)
        cutoff = int(math.ceil(n * self.percentile))
        return cast(List[str], amount_series.iloc[cutoff:].index.tolist())


class HSExchangeSelector(Selector):
    """Select stocks listed on SSE/SZSE."""

    def __init__(self, stock_info: pd.DataFrame, parents: Optional[List[Selector]] = None) -> None:
        super().__init__(parents)
        self.stock_info = stock_info

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        hs_exchange_stocks = set(
            self.stock_info[self.stock_info["exchange"].isin(["SSE", "SZSE"])].index
        )
        if universe is None:
            return []
        return cast(List[str], list(hs_exchange_stocks.intersection(universe)))


class ListedMoreThanOneYearSelector(Selector):
    """Select stocks listed more than one year."""

    def __init__(
        self,
        stock_info: pd.DataFrame,
        asof_date: pd.Timestamp,
        parents: Optional[List[Selector]] = None,
    ) -> None:
        super().__init__(parents)
        self.stock_info = stock_info
        self.asof_date = asof_date

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        listed = self.stock_info[
            self.stock_info["listing_date"] <= self.asof_date - pd.DateOffset(years=1)
        ].index
        if universe is None:
            return []
        return cast(List[str], list(listed.intersection(universe)))


class HasPriceSelector(Selector):
    """Select stocks with price on asof_date."""

    def __init__(
        self,
        price_df: pd.DataFrame,
        asof_date: pd.Timestamp,
        parents: Optional[List[Selector]] = None,
    ) -> None:
        super().__init__(parents)
        self.price_df = price_df
        self.asof_date = asof_date

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        if self.price_df is None or self.asof_date not in self.price_df.index:
            return []
        available = self.price_df.columns[self.price_df.loc[self.asof_date].notna()]
        if universe is None:
            return []
        return cast(List[str], list(set(available).intersection(universe)))


class ExcludeBlacklistSelector(Selector):
    """Exclude blacklisted stocks."""

    def __init__(self, blacklist: List[str], parents: Optional[List[Selector]] = None) -> None:
        super().__init__(parents)
        self.blacklist = set(blacklist)

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        if universe is None:
            return []
        return list(set(universe) - self.blacklist)


class BasicSelector(Selector):
    """Composite selector for base universe filtering."""

    def __init__(
        self,
        stock_info: pd.DataFrame,
        blacklist: List[str],
        price_df: pd.DataFrame,
        asof_date: pd.Timestamp,
    ) -> None:
        super().__init__(
            parents=[
                ListedMoreThanOneYearSelector(stock_info, asof_date),
                HSExchangeSelector(stock_info),
                HasPriceSelector(price_df, asof_date),
                ExcludeBlacklistSelector(blacklist),
            ]
        )

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        return list(universe) if universe else []


class MktCapPercentileSelector(Selector):
    """Select stocks by market cap percentile."""

    def __init__(
        self,
        mkt_cap_df: pd.DataFrame,
        asof_date: pd.Timestamp,
        percentile: tuple[float, float],
        parents: Optional[List[Selector]] = None,
    ) -> None:
        super().__init__(parents)
        self.mkt_cap_df = mkt_cap_df
        self.asof_date = asof_date
        self.percentile = percentile

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        if self.mkt_cap_df is None or self.asof_date not in self.mkt_cap_df.index:
            return []
        cap_series = self.mkt_cap_df.loc[self.asof_date].dropna()
        if universe is not None:
            cap_series = cap_series[cap_series.index.isin(universe)]
        sorted_caps = cap_series.sort_values(ascending=True)
        n = len(sorted_caps)
        if n == 0:
            return []
        lower, upper = int(n * self.percentile[0]), int(n * self.percentile[1])
        return cast(List[str], sorted_caps.iloc[lower:upper].index.tolist())


class QualityScoreSelector(Selector):
    """Select stocks by quality score."""

    def __init__(
        self,
        stock_info: pd.DataFrame,
        fundamental_df: pd.DataFrame,
        asof_date: pd.Timestamp,
        score_percentile: tuple[float, float] = (0.7, 1.0),
        lookback_range: int = 90,
        parents: Optional[List[Selector]] = None,
    ) -> None:
        super().__init__(parents)
        self.stock_info = stock_info
        self.fundamental_df = fundamental_df
        self.asof_date = asof_date
        self.score_percentile = score_percentile
        self.lookback_range = lookback_range

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        if self.fundamental_df is None:
            return []

        lookback_date = self.asof_date - pd.Timedelta(days=self.lookback_range)
        fundamentals = get_latest_available_row(
            self.fundamental_df,
            lookback_date,
            [
                "operating_profit_ttm",
                "total_equity",
                "net_cash_from_operating",
                "net_profit",
                "total_assets",
                "total_liabilities",
            ],
        )
        if fundamentals.empty:
            return []

        fundamentals = fundamentals.dropna(
            subset=[
                "operating_profit_ttm",
                "total_equity",
                "net_cash_from_operating",
                "net_profit",
                "total_assets",
                "total_liabilities",
            ]
        )
        if fundamentals.empty:
            return []

        fundamentals["profit"] = fundamentals["operating_profit_ttm"] / fundamentals["total_equity"]
        fundamentals["cfq"] = fundamentals["net_cash_from_operating"] / fundamentals["net_profit"]
        fundamentals["lev"] = fundamentals["total_liabilities"] / fundamentals["total_assets"]

        fundamentals["industry"] = self.stock_info.reindex(fundamentals.index)["industry"]

        def zscore_trimmed(group: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
            result = pd.DataFrame(index=group.index, columns=cols)
            for col in cols:
                series = group[col]
                if series.count() <= 2:
                    mu = series.mean()
                    sigma = series.std()
                else:
                    drop_idx = [series.idxmin(), series.idxmax()]
                    trimmed = series.drop(index=drop_idx)
                    if trimmed.count() < 1:
                        mu = series.mean()
                        sigma = series.std()
                    else:
                        mu = trimmed.mean()
                        sigma = trimmed.std()

                if sigma == 0 or pd.isna(sigma):
                    result[col] = 0.0
                else:
                    result[col] = (series - mu) / sigma
            return result

        cols = ["profit", "cfq", "lev"]
        zscore_df = (
            fundamentals.groupby("industry")
            .apply(lambda g: zscore_trimmed(g, cols))
            .reset_index(level=0, drop=True)
        )
        if universe is not None:
            zscore_df = zscore_df.loc[zscore_df.index.intersection(universe)]
        score = zscore_df["profit"] + zscore_df["cfq"] - zscore_df["lev"]
        score = score.dropna().sort_values(ascending=True)
        n = len(score)
        if n < 10:
            return []
        lower, upper = int(n * self.score_percentile[0]), int(n * self.score_percentile[1])
        return cast(List[str], score.iloc[lower:upper].index.tolist())


class BMScoreSelector(Selector):
    """Select stocks by BM score."""

    def __init__(
        self,
        fundamental_df: pd.DataFrame,
        mkt_cap_df: pd.DataFrame,
        asof_date: pd.Timestamp,
        bm_percentile: tuple[float, float] = (0.7, 1.0),
        lookback_range: int = 90,
        parents: Optional[List[Selector]] = None,
    ) -> None:
        super().__init__(parents)
        self.fundamental_df = fundamental_df
        self.mkt_cap_df = mkt_cap_df
        self.asof_date = asof_date
        self.bm_percentile = bm_percentile
        self.lookback_range = lookback_range

    def _select(self, universe: Optional[Set[str]]) -> List[str]:
        if (
            self.fundamental_df is None
            or self.mkt_cap_df is None
            or self.asof_date not in self.mkt_cap_df.index
        ):
            return []

        lookback_date = self.asof_date - pd.Timedelta(days=self.lookback_range)
        fundamentals = get_latest_available_row(
            self.fundamental_df,
            lookback_date,
            ["total_equity"],
        )
        if fundamentals.empty:
            return []

        if universe is not None:
            fundamentals = fundamentals.loc[fundamentals.index.intersection(universe)]
        fundamentals = fundamentals.dropna(subset=["total_equity"])
        if fundamentals.empty:
            return []

        cap_series = self.mkt_cap_df.loc[self.asof_date].reindex(fundamentals.index)
        cap_series = cap_series[cap_series > 1e-6]
        fundamentals = fundamentals.loc[cap_series.index]

        bm = fundamentals["total_equity"] / cap_series
        bm = bm.dropna().sort_values(ascending=True)

        n = len(bm)
        if n < 10:
            return []
        lower, upper = int(n * self.bm_percentile[0]), int(n * self.bm_percentile[1])
        return cast(List[str], bm.iloc[lower:upper].index.tolist())
