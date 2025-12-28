from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from core.fetch.errors import NonRetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.http_client.base import HttpClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpFetcher(Fetcher):
    """HTTP fetcher with chunk-level retry support."""

    client: HttpClient
    retry_policy: RetryPolicy
    url: str
    method: str = "GET"
    timeout: float | None = None

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for the given chunk arguments."""

        def operation() -> RawBatch:
            params = _extract_params(chunk_args)
            json_body = _extract_body(chunk_args)
            logger.debug(
                "http fetch request",
                extra={
                    "endpoint": self.url,
                    "timeout_s": self.timeout,
                },
            )
            response = self.client.request(
                method=self.method,
                url=self.url,
                params=params,
                json_body=json_body,
                timeout=self.timeout,
            )
            return _normalize_response(response.json_data)

        return self.retry_policy.execute(operation)


def _extract_params(chunk_args: ChunkArgs) -> Mapping[str, Any] | None:
    params = chunk_args.get("params")
    if params is None:
        return None
    if not isinstance(params, dict):
        raise NonRetryableError("params must be a mapping")
    return params


def _extract_body(chunk_args: ChunkArgs) -> Mapping[str, Any] | None:
    body = chunk_args.get("body")
    if body is None:
        return None
    if not isinstance(body, dict):
        raise NonRetryableError("body must be a mapping")
    return body


def _normalize_response(payload: Any) -> RawBatch:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
        return payload["data"]
    raise NonRetryableError("unexpected response payload")
