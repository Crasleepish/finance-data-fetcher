from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from core.clean.policy import DropDecision, ErrorMode, QuarantineSink, handle_error
from core.pipeline.types import NormalizedBatch, RawBatch


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
        normalized: list[dict[str, Any]] = []
        for raw in raw_batch:
            if not isinstance(raw, dict):
                decision = self._handle(ValueError("record must be a mapping"), {})
                if decision.drop:
                    continue
                raise ValueError("record must be a mapping")
            record = dict(raw)
            try:
                normalized.append(self._normalize_record(record))
            except Exception as exc:
                decision = self._handle(exc, record)
                if decision.drop:
                    continue
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
