from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import cast

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.indexing.index_codes import IndexCodeMapping
from core.pipeline.types import ChunkArgs, RawBatch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AkshareIndexHistFetcher(Fetcher):
    """Fetch csindex history rows for configured indices in a date range."""

    retry_policy: RetryPolicy

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw csindex rows for each configured index code."""
        params = chunk_args.get("params") or {}
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        start_date_compact = _to_yyyymmdd(start_date)
        end_date_compact = _to_yyyymmdd(end_date)
        codes = _require_codes(params.get("codes"))

        rows: list[dict[str, object]] = []
        for mapping in codes:
            batch = self.retry_policy.execute(
                lambda: _safe_csindex(mapping["api_code"], start_date_compact, end_date_compact)
            )
            for item in batch:
                if isinstance(item, dict):
                    item["index_code"] = mapping["index_code"]
            rows.extend(batch)
        logger.info(
            "akshare csindex fetched",
            extra={
                "start_date": start_date_compact,
                "end_date": end_date_compact,
                "index_count": len(codes),
                "row_count": len(rows),
            },
        )
        return rows


def _safe_csindex(symbol: str, start_date: str, end_date: str) -> list[dict[str, object]]:
    try:
        import akshare as ak

        data = ak.stock_zh_index_hist_csindex(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))
    except json.JSONDecodeError as exc:
        logger.warning(
            "akshare csindex json decode error",
            extra={
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
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


def _to_yyyymmdd(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return value
    return value.replace("-", "")
