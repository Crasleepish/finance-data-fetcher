from __future__ import annotations

from types import SimpleNamespace

from core.fetch.retry import RetryPolicy
from infra.fetcher.akshare_hk_index_daily_fetcher import AkshareHkIndexDailyFetcher


class _FakeDataFrame:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.empty = not rows

    def to_dict(self, orient: str) -> list[dict[str, object]]:
        assert orient == "records"
        return list(self._rows)


def test_akshare_hk_index_daily_fetcher_filters_and_sets_code(monkeypatch) -> None:
    # Values sourced from production index_hist row (Au99.95.SGE, 2025-12-31).
    rows = [
        {
            "date": "2025-12-31",
            "open": 952.0,
            "close": 970.8,
            "high": 980.0,
            "low": 952.0,
            "volume": 54000,
        },
        {
            "date": "2024-12-31",
            "open": 950.0,
            "close": 960.0,
            "high": 965.0,
            "low": 940.0,
            "volume": 50000,
        },
    ]
    fake_ak = SimpleNamespace(stock_hk_index_daily_sina=lambda symbol: _FakeDataFrame(rows))
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)

    fetcher = AkshareHkIndexDailyFetcher(retry_policy=RetryPolicy())
    result = fetcher.fetch(
        {
            "params": {
                "start_date": "2025-12-01",
                "end_date": "2025-12-31",
                "codes": [{"index_code": "HSI.GLB", "api_code": "HSI"}],
            }
        }
    )

    assert result == [
        {
            "date": "2025-12-31",
            "open": 952.0,
            "close": 970.8,
            "high": 980.0,
            "low": 952.0,
            "volume": 54000,
            "index_code": "HSI.GLB",
        }
    ]
