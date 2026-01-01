from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from core.clean.etf_hist_cleaner import EtfHistCleaner
from core.fetch.retry import RetryPolicy
from infra.fetcher.tushare_fund_daily_fetcher import TushareFundDailyFetcher


@dataclass
class FakeTushareClient:
    pages: dict[int, list[dict[str, object]]] = field(default_factory=dict)
    calls: list[tuple[int, int]] = field(default_factory=list)

    def fund_daily(
        self, trade_date: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        self.calls.append((offset, limit))
        return self.pages.get(offset, [])


def test_fund_daily_fetcher_paginates_and_filters() -> None:
    client = FakeTushareClient(
        pages={
            0: [{"ts_code": "510300.SH"}, {"ts_code": "OTHER"}],
            2000: [],
        }
    )
    fetcher = TushareFundDailyFetcher(client=client, retry_policy=RetryPolicy(), page_size=2000)
    rows = fetcher.fetch({"params": {"trade_date": "2024-01-02", "codes": ["510300.SH"]}})

    assert [row["ts_code"] for row in rows] == ["510300.SH"]
    assert client.calls == [(0, 2000), (2000, 2000)]


def test_etf_hist_cleaner_maps_and_converts() -> None:
    cleaner = EtfHistCleaner()
    raw = [
        {
            "ts_code": "510300.SH",
            "trade_date": "20240102",
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "change": 0.05,
            "pct_chg": 4.5,
            "vol": 2.0,
            "amount": 3.0,
        },
        {
            "ts_code": "510500.SH",
            "trade_date": "20240102",
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "change": 0.05,
            "pct_chg": 4.5,
            "vol": None,
            "amount": None,
        },
    ]

    cleaned = list(cleaner.clean(raw))

    assert cleaned == [
        {
            "etf_code": "510300.SH",
            "date": date(2024, 1, 2),
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "change": 0.05,
            "change_percent": 4.5,
            "volume": 200,
            "amount": 3000.0,
        },
        {
            "etf_code": "510500.SH",
            "date": date(2024, 1, 2),
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "change": 0.05,
            "change_percent": 4.5,
            "volume": None,
            "amount": None,
        },
    ]
