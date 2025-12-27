from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class HttpResponse:
    """Normalized HTTP response payload."""

    status_code: int
    json_data: Any


class HttpClient(Protocol):
    """HTTP client interface used by fetchers."""

    def request(
        self,
        method: str,
        url: str,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """Send an HTTP request and return normalized response data."""
