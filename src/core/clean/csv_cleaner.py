from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.clean.policy import ErrorMode
from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


@dataclass(frozen=True)
class CsvMessageCleaner:
    """Normalize CSV rows into test_messages records."""

    error_mode: ErrorMode = ErrorMode.FAIL_CHUNK

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        cleaner = TypedCleaner(
            field_map={"id": "id", "message": "message"},
            type_map={"id": int, "message": str},
            required_fields={"id", "message"},
            casts={"id": _to_int, "message": str},
            error_mode=self.error_mode,
        )
        return cleaner.clean(raw_batch)


def _to_int(value: Any) -> int:
    return int(value)
