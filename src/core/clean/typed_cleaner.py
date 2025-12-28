from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from core.clean.policy import DropDecision, ErrorMode, QuarantineSink, handle_error
from core.pipeline.types import NormalizedBatch, RawBatch

logger = logging.getLogger(__name__)
_QUARANTINE_WARN_THRESHOLD = 0.1


@dataclass(frozen=True)
class TypedCleaner:
    """Cleaner that validates and normalizes dict-based records."""

    field_map: Mapping[str, str]
    type_map: Mapping[str, type | tuple[type, ...]]
    required_fields: set[str] = field(default_factory=set)
    casts: Mapping[str, Callable[[Any], Any]] = field(default_factory=dict)
    error_mode: ErrorMode = ErrorMode.FAIL_CHUNK
    quarantine: QuarantineSink | None = None

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Validate and normalize raw records into DB-ready records."""
        started_at = time.monotonic()
        raw_count = len(raw_batch)
        normalized: list[dict[str, Any]] = []
        dropped_count = 0
        quarantined_count = 0
        error_codes: Counter[str] = Counter()
        sample_errors: list[dict[str, str]] = []
        for raw in raw_batch:
            if not isinstance(raw, dict):
                decision = self._handle(ValueError("record must be a mapping"), {})
                if decision.drop:
                    dropped_count += 1
                    if decision.quarantined:
                        quarantined_count += 1
                    error_codes.update(["record_not_mapping"])
                    if len(sample_errors) < 3:
                        sample_errors.append(
                            {
                                "error_code": "record_not_mapping",
                                "message": "record must be a mapping",
                            }
                        )
                    continue
                raise ValueError("record must be a mapping")
            record = dict(raw)
            try:
                normalized.append(self._normalize_record(record))
            except Exception as exc:
                decision = self._handle(exc, record)
                if decision.drop:
                    dropped_count += 1
                    if decision.quarantined:
                        quarantined_count += 1
                    error_code = type(exc).__name__
                    error_codes.update([error_code])
                    if len(sample_errors) < 3:
                        sample_errors.append({"error_code": error_code, "message": str(exc)})
                    continue
        duration_ms = int((time.monotonic() - started_at) * 1000)
        normalized_count = len(normalized)
        logger.info(
            "clean completed",
            extra={
                "raw_count": raw_count,
                "normalized_count": normalized_count,
                "dropped_count": dropped_count,
                "quarantined_count": quarantined_count,
                "duration_ms": duration_ms,
            },
        )
        if raw_count > 0 and quarantined_count / raw_count >= _QUARANTINE_WARN_THRESHOLD:
            top_errors = [code for code, _ in error_codes.most_common(3)]
            logger.warning(
                "quarantine rate high",
                extra={
                    "quarantine_rate": round(quarantined_count / raw_count, 4),
                    "top_error_codes": top_errors,
                },
            )
        if sample_errors:
            logger.debug("quarantine samples", extra={"sample_errors": sample_errors})
        return normalized

    def _normalize_record(self, raw: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for raw_key, target_key in self.field_map.items():
            if raw_key not in raw:
                if target_key in self.required_fields:
                    raise ValueError(f"missing required field: {target_key}")
                continue
            value = raw[raw_key]
            if target_key in self.casts:
                value = self.casts[target_key](value)
            if target_key in self.type_map:
                expected = self.type_map[target_key]
                if not isinstance(value, expected):
                    raise ValueError(f"invalid type for {target_key}")
            normalized[target_key] = value
        return normalized

    def _handle(self, error: Exception, raw: dict[str, Any]) -> DropDecision:
        return handle_error(self.error_mode, error, raw, self.quarantine)
