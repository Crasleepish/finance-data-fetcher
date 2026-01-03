from __future__ import annotations

from typing import Any, Mapping

from core.pipeline.types import NormalizedBatch, RawBatch


class GoldFutureCurveCleaner:
    """Normalize gold futures curve records for direct DB persistence."""

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Return raw futures curve records as normalized dicts."""
        return [dict(_normalize_record(item)) for item in raw_batch]


def _normalize_record(item: Mapping[str, Any]) -> Mapping[str, Any]:
    return dict(item)
