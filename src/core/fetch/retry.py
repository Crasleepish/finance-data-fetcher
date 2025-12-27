from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

from core.fetch.errors import FetchError, RetryableError

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff retry policy with optional jitter."""

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 5.0
    jitter_ratio: float = 0.2
    sleep_fn: Callable[[float], None] = time.sleep
    random_fn: Callable[[], float] = random.random

    def execute(self, operation: Callable[[], T]) -> T:
        """Execute operation with retry on RetryableError."""
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")

        attempt = 0
        last_error: FetchError | None = None
        while attempt < self.max_attempts:
            attempt += 1
            try:
                return operation()
            except RetryableError as exc:
                last_error = exc
                if attempt >= self.max_attempts:
                    break
                delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
                jitter = delay * self.jitter_ratio * self.random_fn()
                self.sleep_fn(delay + jitter)
        if last_error is None:
            raise FetchError("retry policy failed without error")
        raise last_error
