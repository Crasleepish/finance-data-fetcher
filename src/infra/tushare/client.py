from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, cast

import tushare as ts


class TushareClient(Protocol):
    """Tushare client interface used by fetchers."""

    def stock_basic(
        self,
        exchange: str,
        list_status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return stock_basic rows as a list of dicts."""


@dataclass(frozen=True)
class TushareProClient(TushareClient):
    """Tushare PRO client wrapper."""

    token: str

    def stock_basic(
        self,
        exchange: str,
        list_status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Query stock_basic via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        pro = ts.pro_api(self.token)
        data = pro.stock_basic(
            exchange=exchange,
            list_status=list_status,
            fields=fields,
            offset=offset,
            limit=limit,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))
