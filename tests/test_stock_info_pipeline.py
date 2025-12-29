from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from core.clean.stock_info_cleaner import StockInfoCleaner
from core.fetch.retry import RetryPolicy
from infra.fetcher.tushare_stock_basic_fetcher import TushareStockBasicFetcher


@dataclass
class FakeTushareClient:
    pages: dict[tuple[str, int], list[dict[str, object]]] = field(default_factory=dict)

    def stock_basic(
        self,
        exchange: str,
        list_status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        return self.pages.get((list_status, offset), [])


def test_stock_info_fetcher_paginates_until_empty() -> None:
    client = FakeTushareClient(
        pages={
            ("L", 0): [{"ts_code": "000001.SZ"}],
            ("L", 4000): [],
            ("D", 0): [{"ts_code": "000002.SZ"}, {"ts_code": "000003.SZ"}],
            ("D", 4000): [],
            ("P", 0): [],
        }
    )
    fetcher = TushareStockBasicFetcher(client=client, retry_policy=RetryPolicy(), page_size=4000)
    rows = fetcher.fetch({"params": {"list_statuses": ["L", "D", "P"], "exchange": ""}})
    assert [row["ts_code"] for row in rows] == ["000001.SZ", "000002.SZ", "000003.SZ"]


def test_stock_info_cleaner_maps_fields() -> None:
    cleaner = StockInfoCleaner()
    raw = [
        {
            "ts_code": "000001.SZ",
            "name": "Ping An Bank",
            "market": "Main",
            "exchange": "SZSE",
            "industry": "Bank",
            "list_date": "19910403",
            "list_status": "L",
        }
    ]
    cleaned = list(cleaner.clean(raw))
    assert cleaned == [
        {
            "stock_code": "000001.SZ",
            "stock_name": "Ping An Bank",
            "market": "Main",
            "exchange": "SZSE",
            "industry": "Bank",
            "listing_date": date(1991, 4, 3),
            "list_status": "L",
        }
    ]
