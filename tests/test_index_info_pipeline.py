from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from core.clean.index_info_cleaner import IndexInfoCleaner
from core.fetch.retry import RetryPolicy
from infra.fetcher.tushare_index_basic_fetcher import TushareIndexBasicFetcher


@dataclass
class FakeTushareClient:
    pages: dict[tuple[str, int], list[dict[str, object]]] = field(default_factory=dict)
    calls: list[tuple[str, int, int]] = field(default_factory=list)

    def index_basic(
        self, market: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        self.calls.append((market, offset, limit))
        return self.pages.get((market, offset), [])


def test_index_basic_fetcher_paginates_and_loads_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "additional_index_info.csv"
    csv_path.write_text("X00001.CSI,Index X,CSI\n", encoding="utf-8")

    client = FakeTushareClient(
        pages={
            ("CSI", 0): [{"ts_code": "000001.SZ", "name": "Index A", "market": "SSE"}],
            ("CSI", 4000): [],
        }
    )
    fetcher = TushareIndexBasicFetcher(client=client, retry_policy=RetryPolicy(), page_size=4000)
    raw = fetcher.fetch({"params": {"markets": ["CSI"], "csv_path": str(csv_path)}})

    assert raw == [
        {
            "tushare": [{"ts_code": "000001.SZ", "name": "Index A", "market": "SSE"}],
            "csv": [{"ts_code": "X00001.CSI", "name": "Index X", "market": "CSI"}],
        }
    ]
    assert client.calls == [("CSI", 0, 4000), ("CSI", 4000, 4000)]


def test_index_info_cleaner_csv_overrides() -> None:
    cleaner = IndexInfoCleaner()
    raw = [
        {
            "tushare": [
                {"ts_code": "000001.SZ", "name": "Index A", "market": "SSE"},
                {"ts_code": "000002.SZ", "name": "Index B", "market": "SZSE"},
            ],
            "csv": [{"ts_code": "000001.SZ", "name": "Index A+", "market": "CSI"}],
        }
    ]

    cleaned = list(cleaner.clean(raw))

    assert cleaned == [
        {"index_code": "000001.SZ", "index_name": "Index A+", "market": "CSI"},
        {"index_code": "000002.SZ", "index_name": "Index B", "market": "SZSE"},
    ]
