from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

import requests

from core.fetch.errors import NonRetryableError

logger = logging.getLogger(__name__)

_ENDPOINT_PATH = "/api/v1/market/data/stk-code-change"
_CODE_PATTERN = re.compile(r"[0-9]{6}\.(?:SH|SZ|BJ)")
_PAGINATION_FLAGS = (
    "has_more",
    "has_next",
    "is_truncated",
    "truncated",
    "next_page",
    "next_cursor",
    "next",
    "pagination",
)
_PARTIAL_FLAGS = ("partial", "is_partial", "errors", "error_count", "failed")
_PAGE_FIELDS = ("pageNum", "page_num", "pages", "page_count")
_RECORD_KEYS = ("records", "list", "items", "rows", "data")
_PK_FIELDS = ("trade_code", "code", "start_date")
_REDIRECT_STATUS = {301, 302, 303, 307, 308}


@dataclass
class FtshareCodeChangeFetcher:
    """Fetch full A-share code-change snapshots from FTShare.

    The fetcher only plans and transmits raw snapshot requests; suffix, date and
    global validation belong to the consuming pipeline. The API key is never
    logged or exposed through ``repr``.
    """

    api_key: str = field(default="", repr=False)
    base_url: str = "https://market.ft.tech/gateway"
    batch_size: int = 400
    max_url_length: int = 5000
    max_requests: int = 50
    timeout: float = 30.0
    transport: Any = field(default=None, repr=False, compare=False)
    _last_request_count: int = field(default=0, init=False, repr=False, compare=False)
    _planned_batch_count: int = field(default=0, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _validate_constructor_caps(
            self.batch_size, self.max_url_length, self.max_requests, self.timeout
        )
        self.base_url = (
            self.base_url.rstrip("/") if isinstance(self.base_url, str) else self.base_url
        )
        _validate_base_url(self.base_url)
        if self.transport is None:
            self.transport = requests.Session()

    @property
    def last_request_count(self) -> int:
        """Number of attempts actually transmitted in the last ``fetch`` call."""
        return self._last_request_count

    @property
    def planned_batch_count(self) -> int:
        """Number of base batches planned for the last ``fetch`` call."""
        return self._planned_batch_count

    def fetch(
        self,
        codes: Sequence[str],
        *,
        cancel_check: Callable[[], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch raw code-change records for the deduplicated candidate codes."""
        self._last_request_count = 0
        candidates = _dedupe_valid_codes(codes)
        batches = self._plan_batches(candidates)
        self._planned_batch_count = len(batches)

        worst_case = sum(3 if len(batch) > 1 else 1 for batch in batches)
        if worst_case > self.max_requests:
            raise NonRetryableError("ftshare planned request budget exceeds max_requests")
        if batches and not self.api_key:
            raise NonRetryableError("ftshare api_key is not configured")

        rows: list[dict[str, Any]] = []
        for batch in batches:
            rows.extend(self._fetch_batch(batch, cancel_check))
        return rows

    def _plan_batches(self, codes: list[str]) -> list[list[str]]:
        batches: list[list[str]] = []
        current: list[str] = []
        for code in codes:
            trial = current + [code]
            if len(trial) <= self.batch_size and len(self._build_url(trial)) <= self.max_url_length:
                current = trial
                continue
            if not current:
                raise NonRetryableError("ftshare code exceeds URL length limit")
            batches.append(current)
            current = [code]
            if len(self._build_url(current)) > self.max_url_length:
                raise NonRetryableError("ftshare code exceeds URL length limit")
        if current:
            batches.append(current)
        return batches

    def _build_url(self, codes: Sequence[str]) -> str:
        request = requests.Request(
            "GET",
            self.base_url + _ENDPOINT_PATH,
            params={"trade_code": ",".join(codes)},
        )
        url = request.prepare().url or ""
        parsed = urlsplit(url)
        base = urlsplit(self.base_url)
        if parsed.scheme != "https" or parsed.netloc != base.netloc:
            raise NonRetryableError("ftshare prepared url escaped base origin")
        return url

    def _transmit(self, url: str, cancel_check: Callable[[], bool] | None) -> Any:
        if cancel_check is not None and cancel_check():
            raise NonRetryableError("ftshare fetch cancelled")
        if self._last_request_count >= self.max_requests:
            raise NonRetryableError("ftshare request budget exhausted")
        self._last_request_count += 1
        try:
            response = self.transport.get(
                url,
                headers={
                    "FTSHARE_API_KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
                allow_redirects=False,
            )
        except requests.Timeout:
            raise NonRetryableError("ftshare request timeout") from None
        except requests.RequestException:
            raise NonRetryableError("ftshare request failed") from None

        status = response.status_code
        if status in _REDIRECT_STATUS:
            raise NonRetryableError("ftshare redirect rejected")
        if status == 414:
            raise NonRetryableError("ftshare url too long")
        if status in {401, 403}:
            raise NonRetryableError("ftshare auth failed")
        if status >= 400:
            raise NonRetryableError("ftshare request error")
        try:
            return response.json()
        except ValueError:
            raise NonRetryableError("ftshare invalid json") from None

    def _fetch_batch(
        self, batch: list[str], cancel_check: Callable[[], bool] | None
    ) -> list[dict[str, Any]]:
        records = self._request_records(batch, set(batch), cancel_check)
        if len(batch) == 1 or not records:
            return records
        half = len(batch) // 2
        left, right = batch[:half], batch[half:]
        left_records = self._request_records(left, set(left), cancel_check)
        right_records = self._request_records(right, set(right), cancel_check)
        if _canonical_counter(records) != _canonical_counter(left_records + right_records):
            raise NonRetryableError("ftshare verification mismatch")
        return records

    def _request_records(
        self,
        batch: Sequence[str],
        queried: set[str],
        cancel_check: Callable[[], bool] | None,
    ) -> list[dict[str, Any]]:
        payload = self._transmit(self._build_url(batch), cancel_check)
        return _parse_records(payload, queried)


def _validate_constructor_caps(
    batch_size: Any, max_url_length: Any, max_requests: Any, timeout: Any
) -> None:
    _require_int_in_range("batch_size", batch_size, 1, 400)
    _require_int_in_range("max_url_length", max_url_length, 1, 5000)
    _require_int_in_range("max_requests", max_requests, 1, 50)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise NonRetryableError("ftshare timeout must be a positive number")


def _require_int_in_range(name: str, value: Any, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise NonRetryableError(f"ftshare {name} must be between {minimum} and {maximum}")


def _validate_base_url(base_url: Any) -> None:
    if not isinstance(base_url, str):
        raise NonRetryableError("ftshare base_url must be a string")
    parsed = urlsplit(base_url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise NonRetryableError("ftshare base_url must be a secure https origin")
    if parsed.username or parsed.password:
        raise NonRetryableError("ftshare base_url must not contain userinfo")
    if parsed.query:
        raise NonRetryableError("ftshare base_url must not contain a query")
    if parsed.fragment:
        raise NonRetryableError("ftshare base_url must not contain a fragment")
    try:
        parsed.port
    except ValueError:
        raise NonRetryableError("ftshare base_url has an invalid port") from None


def _dedupe_valid_codes(codes: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in codes:
        if not isinstance(raw, str):
            raise NonRetryableError("ftshare code must be a string")
        code = raw.strip()
        if not code:
            raise NonRetryableError("ftshare code is blank")
        if _CODE_PATTERN.fullmatch(code) is None:
            raise NonRetryableError("ftshare code has invalid syntax")
        if code in seen:
            continue
        seen.add(code)
        result.append(code)
    return result


def _parse_records(payload: Any, queried: set[str]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise NonRetryableError("ftshare unexpected payload")
    if payload.get("code") != 200:
        raise NonRetryableError("ftshare envelope error")
    _check_unsupported_flags(payload)

    data = payload.get("data")
    total = payload.get("total")
    if isinstance(data, list):
        records: list[Any] = data
    elif isinstance(data, dict):
        _check_unsupported_flags(data)
        records = []
        found = False
        for key in _RECORD_KEYS:
            value = data.get(key)
            if isinstance(value, list):
                records = value
                found = True
                break
        if not found:
            raise NonRetryableError("ftshare unexpected data schema")
        if "total" in data:
            total = data["total"]
    else:
        raise NonRetryableError("ftshare unexpected data schema")

    if total is not None:
        if _coerce_total(total) != len(records):
            raise NonRetryableError("ftshare total mismatch")

    for row in records:
        if not isinstance(row, dict):
            raise NonRetryableError("ftshare unexpected record")
        trade_code = row.get("trade_code")
        if not isinstance(trade_code, str) or not trade_code:
            raise NonRetryableError("ftshare missing trade_code")
        if trade_code not in queried:
            raise NonRetryableError("ftshare returned unexpected trade_code")

    _assert_no_conflicting_pk(records)
    return records


def _check_unsupported_flags(mapping: Mapping[str, Any]) -> None:
    for flag in _PAGINATION_FLAGS:
        if mapping.get(flag):
            raise NonRetryableError("ftshare pagination or truncation not supported")
    for flag in _PARTIAL_FLAGS:
        if mapping.get(flag):
            raise NonRetryableError("ftshare partial response not supported")
    for field_name in _PAGE_FIELDS:
        value = mapping.get(field_name)
        if value is not None and not isinstance(value, bool) and value > 1:
            raise NonRetryableError("ftshare pagination or truncation not supported")


def _coerce_total(total: Any) -> int:
    if isinstance(total, bool):
        raise NonRetryableError("ftshare invalid total")
    if isinstance(total, int):
        return total
    if isinstance(total, float) and total.is_integer():
        return int(total)
    if isinstance(total, str) and total.strip().isdigit():
        return int(total)
    raise NonRetryableError("ftshare invalid total")


def _assert_no_conflicting_pk(records: Sequence[Any]) -> None:
    seen: dict[tuple[str, ...], Any] = {}
    for row in records:
        if not isinstance(row, Mapping) or not all(key in row for key in _PK_FIELDS):
            continue
        pk = tuple(str(row[key]) for key in _PK_FIELDS)
        canonical = _canonical_row(row)
        existing = seen.get(pk)
        if existing is None:
            seen[pk] = canonical
        elif existing != canonical:
            raise NonRetryableError("ftshare conflicting duplicate record")


def _canonical_counter(rows: Sequence[Any]) -> Counter[Any]:
    return Counter(_canonical_row(row) for row in rows)


def _canonical_row(row: Any) -> Any:
    if isinstance(row, Mapping):
        return tuple(sorted((str(key), _canonical_row(value)) for key, value in row.items()))
    if isinstance(row, (list, tuple)):
        return tuple(_canonical_row(item) for item in row)
    return row
