from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from infra import xueqiu_token_fetcher
from infra.xueqiu_token_fetcher import XueqiuTokenFetcher


def test_xueqiu_token_fetcher_mocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixed_time = datetime(2025, 1, 2, 3, 4, 5)
    token_value = "token-abc123"

    class FakeSession:
        def __init__(self) -> None:
            self.cookies = {}

        def get(self, url: str, headers: dict[str, str], timeout: float) -> object:
            if url.endswith("/service/csrf"):
                self.cookies["xq_a_token"] = token_value
            response = SimpleNamespace()
            response.raise_for_status = Mock()
            return response

    monkeypatch.setattr(xueqiu_token_fetcher, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(xueqiu_token_fetcher, "datetime", SimpleNamespace(now=lambda: fixed_time))
    monkeypatch.setattr(xueqiu_token_fetcher.requests, "Session", FakeSession)

    fetcher = XueqiuTokenFetcher(timeout_s=1.0)
    token = fetcher.fetch_token()
    assert token == token_value

    path = fetcher.fetch_and_store()
    assert path == tmp_path / "xq_a_token_20250102030405.txt"
    assert path.read_text(encoding="utf-8").strip() == token_value
