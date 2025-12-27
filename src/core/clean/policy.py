from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class ErrorMode(StrEnum):
    """Error handling modes for cleaner validation failures."""

    FAIL_CHUNK = "fail_chunk"
    DROP_RECORD = "drop_record"
    QUARANTINE = "quarantine"


class QuarantineSink(Protocol):
    """Sink for quarantined records."""

    def record(self, error: Exception, raw: dict[str, Any]) -> None: ...


@dataclass(frozen=True)
class DropDecision:
    """Decision result for a validation error."""

    drop: bool


def handle_error(
    mode: ErrorMode,
    error: Exception,
    raw: dict[str, Any],
    quarantine: QuarantineSink | None = None,
) -> DropDecision:
    """Handle validation errors based on policy."""
    if mode == ErrorMode.FAIL_CHUNK:
        raise error
    if mode == ErrorMode.DROP_RECORD:
        logger.warning("dropping invalid record", extra={"error": str(error)})
        return DropDecision(drop=True)
    if mode == ErrorMode.QUARANTINE:
        if quarantine is not None:
            quarantine.record(error, raw)
        else:
            logger.warning("quarantine sink missing", extra={"error": str(error)})
        return DropDecision(drop=True)
    raise ValueError(f"unknown error mode: {mode}")
