from __future__ import annotations


class FetchError(RuntimeError):
    """Base error raised during fetch operations."""


class RetryableError(FetchError):
    """Transient error that should be retried."""


class NonRetryableError(FetchError):
    """Deterministic error that should not be retried."""
