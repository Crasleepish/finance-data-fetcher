from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.indexing.index_codes import IndexCodeMapping
from core.pipeline.types import ChunkArgs, RawBatch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AkshareHkIndexDailyFetcher(Fetcher):
    """Fetch HK index daily rows from Sina for configured indices."""

    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw HK index rows for each configured index code."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        start = _parse_date_required(start_date)
        end = _parse_date_required(end_date)
        codes = _require_codes(params.get("codes"))

        rows: list[dict[str, object]] = []
        for mapping in codes:
            batch = self.retry_policy.execute(lambda: _safe_hk_index(mapping["api_code"]))
            for item in batch:
                row_date = _parse_date_optional(item.get("date"))
                if row_date is None:
                    continue
                if row_date < start or row_date > end:
                    continue
                item["index_code"] = mapping["index_code"]
                rows.append(item)
        logger.info(
            "akshare hk index fetched",
            extra={
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "index_count": len(codes),
                "row_count": len(rows),
            },
        )
        return rows


def _safe_hk_index(symbol: str) -> list[dict[str, object]]:
    try:
        import akshare as ak

        data = ak.stock_hk_index_daily_sina(symbol=symbol)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))
    except json.JSONDecodeError as exc:
        logger.warning(
            "akshare hk index json decode error",
            extra={
                "symbol": symbol,
                "error": str(exc),
            },
        )
        return []
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _require_codes(value: object) -> list[IndexCodeMapping]:
    if isinstance(value, list) and all(_is_code_mapping(item) for item in value):
        return value
    raise ValueError("codes must be list[IndexCodeMapping]")


def _is_code_mapping(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    return isinstance(value.get("index_code"), str) and isinstance(value.get("api_code"), str)


def _parse_date_optional(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        if len(value) == 8 and value.isdigit():
            return datetime.strptime(value, "%Y%m%d").date()
        return datetime.strptime(value, "%Y-%m-%d").date()
    return None


def _parse_date_required(value: str) -> date:
    parsed = _parse_date_optional(value)
    if parsed is None:
        raise ValueError("expected date string for start_date/end_date")
    return parsed
