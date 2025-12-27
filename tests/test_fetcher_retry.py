from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import pytest

from core.fetch.errors import NonRetryableError, RetryableError
from core.fetch.retry import RetryPolicy
from infra.fetcher.http_fetcher import HttpFetcher
from infra.http_client.base import HttpClient, HttpResponse


@dataclass
class FlakyClient(HttpClient):
    failures_before_success: int
    calls: int = 0
    payload: Any = field(default_factory=lambda: [{"ok": True}])

    def request(
        self,
        method: str,
        url: str,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise RetryableError("temporary failure")
        return HttpResponse(status_code=200, json_data=self.payload)


def test_retry_succeeds_after_failures() -> None:
    client = FlakyClient(failures_before_success=2)
    policy = RetryPolicy(max_attempts=3, sleep_fn=lambda _: None, random_fn=lambda: 0)
    fetcher = HttpFetcher(client=client, retry_policy=policy, url="http://example")

    result = fetcher.fetch({"params": {"page": 1}})

    assert client.calls == 3
    assert result == [{"ok": True}]


def test_retry_exhaustion_raises() -> None:
    client = FlakyClient(failures_before_success=5)
    policy = RetryPolicy(max_attempts=2, sleep_fn=lambda _: None, random_fn=lambda: 0)
    fetcher = HttpFetcher(client=client, retry_policy=policy, url="http://example")

    def workflow_capture() -> str:
        try:
            fetcher.fetch({"params": {"page": 1}})
        except RetryableError:
            return "failed"
        return "unexpected"

    assert workflow_capture() == "failed"


def test_retry_exhaustion_raises_error() -> None:
    client = FlakyClient(failures_before_success=5)
    policy = RetryPolicy(max_attempts=2, sleep_fn=lambda _: None, random_fn=lambda: 0)
    fetcher = HttpFetcher(client=client, retry_policy=policy, url="http://example")

    with pytest.raises(RetryableError):
        fetcher.fetch({"params": {"page": 1}})


def test_response_normalization() -> None:
    client = FlakyClient(failures_before_success=0, payload={"data": [{"id": 1}]})
    policy = RetryPolicy(max_attempts=1, sleep_fn=lambda _: None, random_fn=lambda: 0)
    fetcher = HttpFetcher(client=client, retry_policy=policy, url="http://example")

    result = fetcher.fetch({"params": {"page": 1}})

    assert result == [{"id": 1}]


@pytest.mark.parametrize("payload", ["bad", 123, {"data": "bad"}])
def test_response_normalization_invalid(payload: Any) -> None:
    client = FlakyClient(failures_before_success=0, payload=payload)
    policy = RetryPolicy(max_attempts=1, sleep_fn=lambda _: None, random_fn=lambda: 0)
    fetcher = HttpFetcher(client=client, retry_policy=policy, url="http://example")

    with pytest.raises(NonRetryableError):
        fetcher.fetch({"params": {"page": 1}})
