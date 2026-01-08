from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,name,market,exchange,industry,list_date,list_status"


@dataclass(frozen=True)
class TushareStockBasicFetcher(Fetcher):
    """Fetch stock_basic data from Tushare with internal pagination."""

    client: TushareClient
    retry_policy: RetryPolicy
    page_size: int = 4000

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw data for the given chunk arguments."""
        cancel_check = _get_cancel_check(chunk_args)
        params = chunk_args.get("params") or {}
        exchange = str(params.get("exchange", ""))
        list_statuses = _normalize_list_statuses(params.get("list_statuses"))
        fields = str(params.get("fields", _DEFAULT_FIELDS))

        all_rows: list[dict[str, object]] = []
        for list_status in list_statuses:
            offset = 0
            pages = 0
            while True:
                if cancel_check is not None:
                    cancel_check()
                batch = self.retry_policy.execute(
                    lambda: _safe_stock_basic(
                        self.client,
                        exchange,
                        list_status,
                        fields,
                        offset,
                        self.page_size,
                    )
                )
                if not batch:
                    logger.info(
                        "tushare stock_basic exhausted",
                        extra={
                            "list_status": list_status,
                            "exchange": exchange,
                            "pages": pages,
                            "total_rows": len(all_rows),
                        },
                    )
                    break
                all_rows.extend(batch)
                pages += 1
                logger.debug(
                    "tushare stock_basic page",
                    extra={
                        "list_status": list_status,
                        "exchange": exchange,
                        "offset": offset,
                        "limit": self.page_size,
                        "batch_size": len(batch),
                    },
                )
                offset += self.page_size
        return all_rows


def _safe_stock_basic(
    client: TushareClient,
    exchange: str,
    list_status: str,
    fields: str,
    offset: int,
    limit: int,
) -> list[dict[str, object]]:
    try:
        return client.stock_basic(
            exchange=exchange,
            list_status=list_status,
            fields=fields,
            offset=offset,
            limit=limit,
        )
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _normalize_list_statuses(value: object) -> list[str]:
    if value is None:
        return ["L", "D", "P"]
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    raise ValueError("list_statuses must be list[str] or comma-separated string")


def _get_cancel_check(chunk_args: ChunkArgs) -> Callable[[], None] | None:
    check = chunk_args.get("cancel_check")
    return check if callable(check) else None
