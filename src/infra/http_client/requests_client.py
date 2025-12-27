from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import requests
from requests import Response

from core.fetch.errors import NonRetryableError, RetryableError
from infra.http_client.base import HttpClient, HttpResponse


@dataclass(frozen=True)
class RequestsHttpClient(HttpClient):
    """Requests-based HTTP client adapter."""

    def request(
        self,
        method: str,
        url: str,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                timeout=timeout,
            )
        except requests.Timeout as exc:
            raise RetryableError("request timeout") from exc
        except requests.RequestException as exc:
            raise RetryableError("request failed") from exc

        self._raise_for_status(response)
        return HttpResponse(status_code=response.status_code, json_data=response.json())

    @staticmethod
    def _raise_for_status(response: Response) -> None:
        if 500 <= response.status_code < 600:
            raise RetryableError(f"server error: {response.status_code}")
        if 400 <= response.status_code < 500:
            raise NonRetryableError(f"client error: {response.status_code}")
