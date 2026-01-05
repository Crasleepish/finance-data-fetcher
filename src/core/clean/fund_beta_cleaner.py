from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from core.pipeline.types import NormalizedBatch, RawBatch


class FundBetaCleaner:
    """Normalize fund beta records for persistence."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Return raw beta records with scalar normalization."""
        return [_normalize_record(dict(item)) for item in raw_batch]


def _normalize_record(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _normalize_value(value) for key, value in item.items()}


def _normalize_value(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    return value
