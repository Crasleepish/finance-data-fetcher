from __future__ import annotations

import pytest

from core.fetch.errors import NonRetryableError
from core.fetch.retry import RetryPolicy
from infra.fetcher.tushare_stock_basic_fetcher import TushareStockBasicFetcher

_NO_RETRY = RetryPolicy(max_attempts=1, sleep_fn=lambda _: None, random_fn=lambda: 0)


class FakeTushareClient:
    def __init__(self, pages: dict[tuple[str, int], list[dict[str, object]]]) -> None:
        self._pages = pages
        self.calls: list[tuple[str, int]] = []

    def stock_basic(
        self,
        exchange: str,
        list_status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append((list_status, offset))
        return list(self._pages.get((list_status, offset), []))


def _fetcher(client: FakeTushareClient, page_size: int) -> TushareStockBasicFetcher:
    return TushareStockBasicFetcher(client=client, retry_policy=_NO_RETRY, page_size=page_size)


def test_multi_page_pagination_preserved() -> None:
    rows_page_0 = [{"ts_code": f"60000{i}.SH"} for i in range(2)]
    rows_page_2 = [{"ts_code": "600003.SH"}]
    client = FakeTushareClient(
        {
            ("L", 0): rows_page_0,
            ("L", 2): rows_page_2,
            ("L", 4): [],
        }
    )
    fetcher = _fetcher(client, page_size=2)

    rows = fetcher.fetch({"params": {"list_statuses": ["L"]}})

    assert rows == rows_page_0 + rows_page_2
    assert client.calls == [("L", 0), ("L", 2), ("L", 4)]


def test_repeated_nonempty_page_aborts_before_extra_calls() -> None:
    repeated = [{"ts_code": "600000.SH"}]
    client = FakeTushareClient(
        {
            ("L", 0): repeated,
            ("L", 2): list(repeated),
            ("L", 4): [{"ts_code": "600001.SH"}],
        }
    )
    fetcher = _fetcher(client, page_size=2)

    with pytest.raises(NonRetryableError, match="repeated page"):
        fetcher.fetch({"params": {"list_statuses": ["L"]}})

    assert client.calls == [("L", 0), ("L", 2)]
    assert ("L", 4) not in client.calls


def test_completeness_uses_current_live_list_status() -> None:
    live = [{"ts_code": "600000.SH"}]
    delisted = [{"ts_code": "000001.SZ"}]
    client = FakeTushareClient({("L", 0): live, ("L", 2): [], ("D", 0): delisted, ("D", 2): []})
    fetcher = _fetcher(client, page_size=2)

    rows = fetcher.fetch({"params": {"list_statuses": ["L", "D"]}})

    assert rows == live + delisted
    assert any(call[0] == "L" for call in client.calls)
    assert any(call[0] == "D" for call in client.calls)
