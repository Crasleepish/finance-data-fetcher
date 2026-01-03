from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, cast

import pandas as pd

logger = logging.getLogger(__name__)


class WeightAllocator(ABC):
    """Abstract base class for weight allocation."""

    @abstractmethod
    def allocate(
        self,
        stock_list: List[str],
        data: Dict[str, pd.DataFrame],
        asof_date: pd.Timestamp,
    ) -> Dict[str, float]:
        """Allocate weights for a list of stocks."""


class EqualWeightAllocator(WeightAllocator):
    """Allocate equal weights across stocks."""

    def allocate(
        self,
        stock_list: List[str],
        data: Dict[str, pd.DataFrame],
        asof_date: pd.Timestamp,
    ) -> Dict[str, float]:
        n = len(stock_list)
        return {s: 1.0 / n for s in stock_list} if n > 0 else {}


class MktCapWeightAllocator(WeightAllocator):
    """Allocate weights proportional to market cap."""

    def allocate(
        self,
        stock_list: List[str],
        data: Dict[str, pd.DataFrame],
        asof_date: pd.Timestamp,
    ) -> Dict[str, float]:
        mkt_cap_df = data.get("mkt_cap")
        if mkt_cap_df is None or asof_date not in mkt_cap_df.index:
            return {}

        cap_series = mkt_cap_df.loc[asof_date][stock_list].dropna()
        total = cap_series.sum()
        if total == 0:
            return {}
        weights = cap_series / total
        return cast(Dict[str, float], weights.to_dict())
