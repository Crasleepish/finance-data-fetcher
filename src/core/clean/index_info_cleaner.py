from __future__ import annotations

from core.clean.typed_cleaner import TypedCleaner
from core.pipeline.types import NormalizedBatch, RawBatch


class IndexInfoCleaner:
    """Normalize index_basic rows and CSV overrides into index_info records."""

    def __init__(self) -> None:
        self._cleaner = TypedCleaner(
            field_map={
                "ts_code": "index_code",
                "name": "index_name",
                "market": "market",
            },
            type_map={
                "index_code": str,
                "index_name": str,
                "market": str,
            },
            required_fields={"index_code", "index_name", "market"},
        )

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Merge Tushare and CSV index info rows with CSV overrides."""
        if not raw_batch:
            return []
        payload = raw_batch[0]
        if not isinstance(payload, dict):
            raise ValueError("index_info payload must be a mapping")

        tushare_rows = _list_of_dicts(payload.get("tushare"))
        csv_rows = _list_of_dicts(payload.get("csv"))

        normalized = list(self._cleaner.clean(tushare_rows))
        overrides = list(self._cleaner.clean(csv_rows))

        by_code = {row["index_code"]: row for row in normalized if "index_code" in row}
        for row in overrides:
            code = row.get("index_code")
            if code:
                by_code[code] = row
        return [by_code[key] for key in sorted(by_code.keys())]


def _list_of_dicts(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    raise ValueError("expected list of dicts")
