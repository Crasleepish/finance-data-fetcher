from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, cast

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = (
    "ts_code,name,fund_type,invest_type,found_date,m_fee,c_fee,market,list_date,delist_date,status"
)
_MANDATORY_FIELDS = ("ts_code", "name", "list_date", "delist_date")


class DuplicateFundCodeError(ValueError):
    """Raised when fund_basic returns the same ts_code more than once."""


@dataclass(frozen=True)
class TushareFundBasicFetcher(Fetcher):
    """Fetch fund_basic data from Tushare with internal pagination."""

    client: TushareClient
    retry_policy: RetryPolicy
    page_size: int = 10000

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_basic rows for every requested status in one call.

        Statuses are paginated one after another and all rows are returned
        together so the caller performs a single clean/persist transaction.
        """
        cancel_check = _get_cancel_check(chunk_args)
        params = chunk_args.get("params") or {}
        market = str(params.get("market", "O"))
        statuses = _normalize_statuses(params.get("statuses", params.get("status", "L")))
        fields = _normalize_fields(params.get("fields", _DEFAULT_FIELDS))

        all_rows: list[dict[str, object]] = []
        seen_codes: set[str] = set()
        for status in statuses:
            offset = 0
            pages = 0
            while True:
                if cancel_check is not None:
                    cancel_check()
                batch = self.retry_policy.execute(
                    lambda: _safe_fund_basic(
                        self.client,
                        market,
                        status,
                        fields,
                        offset,
                        self.page_size,
                    )
                )
                if not batch:
                    logger.info(
                        "tushare fund_basic exhausted",
                        extra={
                            "market": market,
                            "status": status,
                            "pages": pages,
                            "total_rows": len(all_rows),
                        },
                    )
                    break
                _check_duplicates(batch, status, seen_codes)
                all_rows.extend(batch)
                pages += 1
                logger.debug(
                    "tushare fund_basic page",
                    extra={
                        "market": market,
                        "status": status,
                        "offset": offset,
                        "limit": self.page_size,
                        "batch_size": len(batch),
                    },
                )
                offset += self.page_size
        return all_rows


def _safe_fund_basic(
    client: TushareClient,
    market: str,
    status: str,
    fields: str,
    offset: int,
    limit: int,
) -> list[dict[str, object]]:
    try:
        return client.fund_basic(
            market=market,
            status=status,
            fields=fields,
            offset=offset,
            limit=limit,
        )
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _check_duplicates(batch: RawBatch, status: str, seen_codes: set[str]) -> None:
    """Reject rows whose ts_code was already returned by this fetch call."""
    for row in batch:
        code = row.get("ts_code")
        if code is None or (isinstance(code, str) and not code.strip()):
            continue
        key = str(code)
        if key in seen_codes:
            raise DuplicateFundCodeError(
                f"duplicate ts_code {key!r} in fund_basic rows for status {status!r}; "
                "refusing to persist a response with duplicate keys"
            )
        seen_codes.add(key)


def _normalize_statuses(value: object) -> list[str]:
    """Return requested statuses in caller order, without duplicates."""
    if value is None:
        return ["L"]
    if isinstance(value, str):
        raw = value.split(",")
    elif isinstance(value, (list, tuple)):
        raw = [str(item) for item in value]
    else:
        raise ValueError("statuses must be a string or a list of strings")
    statuses: list[str] = []
    for item in raw:
        status = item.strip()
        if status and status not in statuses:
            statuses.append(status)
    if not statuses:
        raise ValueError("statuses must not be empty")
    return statuses


def _normalize_fields(value: object) -> str:
    """Return comma-joined fields with mandatory columns appended if missing."""
    if isinstance(value, str):
        raw = value.split(",")
    elif isinstance(value, (list, tuple)):
        raw = [str(item) for item in value]
    else:
        raise ValueError("fields must be a string or a list of strings")
    fields: list[str] = []
    for item in raw:
        field = item.strip()
        if field and field not in fields:
            fields.append(field)
    for mandatory in _MANDATORY_FIELDS:
        if mandatory not in fields:
            fields.append(mandatory)
    return ",".join(fields)


def _get_cancel_check(chunk_args: ChunkArgs) -> Callable[[], None] | None:
    check = chunk_args.get("cancel_check")
    if callable(check):
        return cast(Callable[[], None], check)
    return None
