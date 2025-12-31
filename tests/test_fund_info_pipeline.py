from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from core.clean.fund_info_cleaner import FundInfoCleaner
from core.fetch.retry import RetryPolicy
from infra.fetcher.tushare_fund_basic_fetcher import TushareFundBasicFetcher


@dataclass
class FakeTushareClient:
    pages: dict[int, list[dict[str, object]]] = field(default_factory=dict)
    calls: list[tuple[int, int]] = field(default_factory=list)

    def fund_basic(
        self,
        market: str,
        status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append((offset, limit))
        return self.pages.get(offset, [])


def test_fund_basic_fetcher_paginates_until_empty() -> None:
    client = FakeTushareClient(
        pages={
            0: [{"ts_code": "000001.OF"}],
            10000: [],
        }
    )
    fetcher = TushareFundBasicFetcher(client=client, retry_policy=RetryPolicy(), page_size=10000)
    rows = fetcher.fetch({"params": {"market": "O", "status": "L"}})
    assert [row["ts_code"] for row in rows] == ["000001.OF"]
    assert client.calls == [(0, 10000), (10000, 10000)]


def test_fund_info_cleaner_maps_and_drops_missing_found_date() -> None:
    cleaner = FundInfoCleaner()
    raw = [
        {
            "ts_code": "000001.OF",
            "name": "Fund A",
            "fund_type": "TypeA",
            "invest_type": "InvestA",
            "found_date": "20240102",
            "m_fee": 0.1,
            "c_fee": 0.01,
            "market": "O",
        },
        {
            "ts_code": "000001.OF",
            "name": "Fund A+",
            "found_date": "20240103",
        },
        {
            "ts_code": "000002.OF",
            "name": "Fund B",
            "found_date": None,
        },
    ]
    cleaned = list(cleaner.clean(raw))
    assert cleaned == [
        {
            "fund_code": "000001.OF",
            "fund_name": "Fund A+",
            "found_date": date(2024, 1, 3),
        }
    ]
