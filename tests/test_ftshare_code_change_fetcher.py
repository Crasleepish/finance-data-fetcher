from __future__ import annotations

import traceback
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import requests

from core.fetch.errors import NonRetryableError
from infra.fetcher.ftshare_code_change_fetcher import FtshareCodeChangeFetcher


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class FakeTransport:
    def __init__(self, handler: Callable[[str], FakeResponse]) -> None:
        self._handler = handler
        self.calls: list[str] = []
        self.headers: list[dict[str, str]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
        allow_redirects: bool,
    ) -> FakeResponse:
        self.calls.append(url)
        self.headers.append(dict(headers))
        return self._handler(url)


def _codes_from_url(url: str) -> list[str]:
    query = parse_qs(urlsplit(url).query)
    return unquote(query["trade_code"][0]).split(",")


def _row(code: str, name: str = "Co") -> dict[str, Any]:
    return {
        "trade_code": code,
        "code": code,
        "name": name,
        "start_date": "20200101",
        "end_date": None,
    }


def _ok(records: list[dict[str, Any]]) -> FakeResponse:
    return FakeResponse(200, {"code": 200, "message": "ok", "data": records})


def _mapping_handler(
    records_by_code: dict[str, list[dict[str, Any]]],
) -> Callable[[str], FakeResponse]:
    def handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        rows = [row for code in codes for row in records_by_code.get(code, [])]
        return _ok(rows)

    return handler


def _fetcher(transport: FakeTransport, **kwargs: Any) -> FtshareCodeChangeFetcher:
    return FtshareCodeChangeFetcher(api_key="key", transport=transport, **kwargs)


def test_fetch_multi_batch_verification_match() -> None:
    records = {"600848.SH": [_row("600848.SH")], "001872.SZ": [_row("001872.SZ")]}
    transport = FakeTransport(_mapping_handler(records))
    fetcher = _fetcher(transport)

    rows = fetcher.fetch(["600848.SH", "001872.SZ"])

    assert len(rows) == 2
    assert fetcher.planned_batch_count == 1
    assert fetcher.last_request_count == 3
    assert transport.headers[0]["FTSHARE_API_KEY"] == "key"


def test_url_encoding_counts_commas() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = _fetcher(transport)

    url = fetcher._build_url(["600848.SH", "001872.SZ"])

    assert "%2C" in url


def test_planning_uses_encoded_url_length() -> None:
    records = {code: [_row(code)] for code in ("600848.SH", "001872.SZ")}
    transport = FakeTransport(_mapping_handler(records))
    probe = _fetcher(transport, max_url_length=5000)
    single_url_length = len(probe._build_url(["600848.SH"]))

    fetcher = _fetcher(transport, max_url_length=single_url_length)
    rows = fetcher.fetch(["600848.SH", "001872.SZ"])

    assert len(rows) == 2
    assert fetcher.planned_batch_count == 2
    assert fetcher.last_request_count == 2


def test_dedupe_and_suffix_validation() -> None:
    transport = FakeTransport(_mapping_handler({"600848.SH": [_row("600848.SH")]}))
    fetcher = _fetcher(transport)

    rows = fetcher.fetch(["600848.SH", "600848.SH"])

    assert len(rows) == 1
    assert fetcher.last_request_count == 1

    with pytest.raises(NonRetryableError):
        fetcher.fetch(["600848"])


def test_empty_codes_no_network() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = FtshareCodeChangeFetcher(transport=transport)

    assert fetcher.fetch([]) == []
    assert transport.calls == []
    assert fetcher.last_request_count == 0


def test_empty_records_no_split() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = _fetcher(transport)

    rows = fetcher.fetch(["600848.SH", "001872.SZ"])

    assert rows == []
    assert fetcher.last_request_count == 1


def test_verification_mismatch_raises() -> None:
    def handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        if len(codes) == 2:
            return _ok([_row(codes[0]), _row(codes[1])])
        if codes[0] == "600848.SH":
            return _ok([_row(codes[0])])
        return _ok([])

    transport = FakeTransport(handler)
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="verification mismatch"):
        fetcher.fetch(["600848.SH", "001872.SZ"])


def test_hidden_truncation_detected() -> None:
    def handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        return _ok([_row(codes[0])])

    transport = FakeTransport(handler)
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="verification mismatch"):
        fetcher.fetch(["600848.SH", "001872.SZ"])


def test_data_object_with_explicit_total() -> None:
    def ok_handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        return FakeResponse(
            200,
            {"code": 200, "data": {"records": [_row(codes[0])], "total": 1}},
        )

    fetcher = _fetcher(FakeTransport(ok_handler))
    assert len(fetcher.fetch(["600848.SH"])) == 1

    def bad_handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        return FakeResponse(
            200,
            {"code": 200, "data": {"records": [_row(codes[0])], "total": 2}},
        )

    bad = _fetcher(FakeTransport(bad_handler))
    with pytest.raises(NonRetryableError, match="total mismatch"):
        bad.fetch(["600848.SH"])


def test_pagination_flag_rejected() -> None:
    transport = FakeTransport(
        lambda url: FakeResponse(200, {"code": 200, "data": [_row("600848.SH")], "has_more": True})
    )
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError):
        fetcher.fetch(["600848.SH"])


def test_unexpected_trade_code_rejected() -> None:
    transport = FakeTransport(lambda url: _ok([_row("999999.SH")]))
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="unexpected trade_code"):
        fetcher.fetch(["600848.SH"])


def test_envelope_error_rejected() -> None:
    transport = FakeTransport(lambda url: FakeResponse(200, {"code": 500, "data": []}))
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="envelope error"):
        fetcher.fetch(["600848.SH"])


@pytest.mark.parametrize("status", [414, 401, 403, 500, 302])
def test_http_errors_rejected(status: int) -> None:
    transport = FakeTransport(lambda url: FakeResponse(status, None))
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError):
        fetcher.fetch(["600848.SH"])

    assert fetcher.last_request_count == 1


def test_timeout_rejected() -> None:
    def handler(url: str) -> FakeResponse:
        raise requests.Timeout("slow")

    fetcher = _fetcher(FakeTransport(handler))

    with pytest.raises(NonRetryableError, match="timeout"):
        fetcher.fetch(["600848.SH"])


def test_cancel_invoked_before_every_request() -> None:
    records = {"600848.SH": [_row("600848.SH")], "001872.SZ": [_row("001872.SZ")]}
    transport = FakeTransport(_mapping_handler(records))
    fetcher = _fetcher(transport)
    calls = {"n": 0}

    def cancel() -> bool:
        calls["n"] += 1
        return False

    fetcher.fetch(["600848.SH", "001872.SZ"], cancel_check=cancel)

    assert calls["n"] == 3


def test_cancel_exception_propagates() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = _fetcher(transport)

    def cancel() -> bool:
        raise RuntimeError("stop now")

    with pytest.raises(RuntimeError, match="stop now"):
        fetcher.fetch(["600848.SH"], cancel_check=cancel)

    assert transport.calls == []


def test_cancel_truthy_aborts() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="cancelled"):
        fetcher.fetch(["600848.SH"], cancel_check=lambda: True)

    assert transport.calls == []


def test_missing_api_key_fails_only_on_fetch() -> None:
    transport = FakeTransport(_mapping_handler({"600848.SH": [_row("600848.SH")]}))
    fetcher = FtshareCodeChangeFetcher(transport=transport)

    with pytest.raises(NonRetryableError, match="api_key"):
        fetcher.fetch(["600848.SH"])

    assert transport.calls == []
    assert fetcher.last_request_count == 0


def test_insecure_base_rejected() -> None:
    with pytest.raises(NonRetryableError):
        FtshareCodeChangeFetcher(api_key="key", base_url="http://market.ft.tech/gateway")


def test_worst_case_budget_rejected_before_network() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = _fetcher(transport, max_requests=2)

    with pytest.raises(NonRetryableError, match="budget"):
        fetcher.fetch(["600848.SH", "001872.SZ"])

    assert transport.calls == []
    assert fetcher.last_request_count == 0


def test_last_request_count_resets_per_fetch() -> None:
    transport = FakeTransport(_mapping_handler({"600848.SH": [_row("600848.SH")]}))
    fetcher = _fetcher(transport)

    fetcher.fetch(["600848.SH"])
    assert fetcher.last_request_count == 1
    fetcher.fetch(["600848.SH"])
    assert fetcher.last_request_count == 1


def test_token_not_in_repr() -> None:
    transport = FakeTransport(_mapping_handler({}))
    fetcher = FtshareCodeChangeFetcher(api_key="secret-token", transport=transport)

    assert "secret-token" not in repr(fetcher)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": 0},
        {"batch_size": 401},
        {"batch_size": True},
        {"max_url_length": 0},
        {"max_url_length": 5001},
        {"max_requests": 0},
        {"max_requests": 51},
        {"timeout": 0},
        {"timeout": -1},
        {"timeout": True},
    ],
)
def test_constructor_cap_violations(kwargs: dict[str, Any]) -> None:
    with pytest.raises(NonRetryableError):
        FtshareCodeChangeFetcher(
            api_key="key", transport=FakeTransport(_mapping_handler({})), **kwargs
        )


@pytest.mark.parametrize(
    "base_url",
    [
        "https://user:pass@market.ft.tech/gateway",
        "https://market.ft.tech/gateway?x=1",
        "https://market.ft.tech/gateway#frag",
        "https://market.ft.tech:notaport/gateway",
    ],
)
def test_base_url_component_violations(base_url: str) -> None:
    with pytest.raises(NonRetryableError) as excinfo:
        FtshareCodeChangeFetcher(api_key="key", base_url=base_url)

    assert base_url not in str(excinfo.value)


@pytest.mark.parametrize(
    "code",
    [
        "60084.SH",
        "6008481.SH",
        "600848.sh",
        "600848.SHX",
        "6008４8.SH",
        "  ",
    ],
)
def test_strict_candidate_syntax(code: str) -> None:
    fetcher = _fetcher(FakeTransport(_mapping_handler({})))

    with pytest.raises(NonRetryableError):
        fetcher.fetch([code])


def test_whitespace_around_valid_code_stripped() -> None:
    transport = FakeTransport(_mapping_handler({"600848.SH": [_row("600848.SH")]}))
    fetcher = _fetcher(transport)

    rows = fetcher.fetch([" 600848.SH "])

    assert len(rows) == 1


def test_missing_trade_code_rejected() -> None:
    transport = FakeTransport(lambda url: _ok([{"code": "600848.SH", "name": "Co"}]))
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="missing trade_code"):
        fetcher.fetch(["600848.SH"])


def test_nonstring_trade_code_rejected() -> None:
    transport = FakeTransport(lambda url: _ok([{"trade_code": 600848, "code": "600848.SH"}]))
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="missing trade_code"):
        fetcher.fetch(["600848.SH"])


def test_paginated_object_without_total_rejected() -> None:
    def handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        return FakeResponse(
            200,
            {"code": 200, "data": {"records": [_row(codes[0])], "pageNum": 2}},
        )

    fetcher = _fetcher(FakeTransport(handler))

    with pytest.raises(NonRetryableError, match="pagination"):
        fetcher.fetch(["600848.SH"])


@pytest.mark.parametrize("total", [2.5, True, "many"])
def test_invalid_total_rejected(total: Any) -> None:
    def handler(url: str) -> FakeResponse:
        codes = _codes_from_url(url)
        return FakeResponse(
            200,
            {"code": 200, "data": {"records": [_row(codes[0])], "total": total}},
        )

    fetcher = _fetcher(FakeTransport(handler))

    with pytest.raises(NonRetryableError):
        fetcher.fetch(["600848.SH"])


def test_partial_flag_rejected() -> None:
    transport = FakeTransport(
        lambda url: FakeResponse(200, {"code": 200, "data": [], "partial": True})
    )
    fetcher = _fetcher(transport)

    with pytest.raises(NonRetryableError, match="partial"):
        fetcher.fetch(["600848.SH"])


def test_conflicting_duplicate_pk_rejected() -> None:
    def handler(url: str) -> FakeResponse:
        return _ok(
            [
                _row("600848.SH", name="Alpha"),
                _row("600848.SH", name="Beta"),
            ]
        )

    fetcher = _fetcher(FakeTransport(handler))

    with pytest.raises(NonRetryableError, match="conflicting duplicate"):
        fetcher.fetch(["600848.SH"])


def test_legit_identical_duplicate_pk_allowed() -> None:
    def handler(url: str) -> FakeResponse:
        return _ok([_row("600848.SH"), _row("600848.SH")])

    fetcher = _fetcher(FakeTransport(handler))

    rows = fetcher.fetch(["600848.SH"])

    assert len(rows) == 2


def test_chained_transport_exception_hides_secret() -> None:
    sentinel = "sentinel-secret-token"

    def handler(url: str) -> FakeResponse:
        raise requests.RequestException(f"boom {sentinel}")

    fetcher = _fetcher(FakeTransport(handler))

    with pytest.raises(NonRetryableError) as excinfo:
        fetcher.fetch(["600848.SH"])

    rendered = "".join(traceback.format_exception(excinfo.value))
    assert sentinel not in rendered
    assert excinfo.value.__cause__ is None
