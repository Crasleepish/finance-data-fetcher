from __future__ import annotations

from datetime import date

import pytest

from core.data_query.mapping import default_currency_unit
from core.data_query.validation import (
    normalize_limit,
    normalize_list_order,
    normalize_page,
    normalize_page_size,
    normalize_results_order,
    parse_date,
    parse_date_range,
    validate_asset_code,
    validate_data_type,
)


def test_parse_date_accepts_iso_and_compact() -> None:
    assert parse_date("2024-01-02") == date(2024, 1, 2)
    assert parse_date("20240102") == date(2024, 1, 2)


@pytest.mark.parametrize("value", ["2024-13-01", "20240199", "not-a-date"])
def test_parse_date_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_date(value)


def test_parse_date_range_validates_order() -> None:
    start, end = parse_date_range("2024-01-01", "2024-01-02")
    assert start == date(2024, 1, 1)
    assert end == date(2024, 1, 2)

    with pytest.raises(ValueError):
        parse_date_range("2024-01-03", "2024-01-02")


@pytest.mark.parametrize("value", ["stock", "fund", "index", "etf"])
def test_validate_data_type_accepts_known(value: str) -> None:
    assert validate_data_type(value) == value


@pytest.mark.parametrize("value", ["", "crypto", "bond"])
def test_validate_data_type_rejects_unknown(value: str) -> None:
    with pytest.raises(ValueError):
        validate_data_type(value)


def test_validate_asset_code_requires_non_empty() -> None:
    assert validate_asset_code("600519") == "600519"
    with pytest.raises(ValueError):
        validate_asset_code("")


def test_normalize_limit_defaults_and_caps() -> None:
    assert normalize_limit(None) == 5000
    assert normalize_limit(1) == 1
    assert normalize_limit(20000) == 20000
    with pytest.raises(ValueError):
        normalize_limit(0)
    with pytest.raises(ValueError):
        normalize_limit(20001)


def test_normalize_page_defaults_and_bounds() -> None:
    assert normalize_page(None) == 1
    assert normalize_page(3) == 3
    with pytest.raises(ValueError):
        normalize_page(0)


def test_normalize_page_size_defaults_and_caps() -> None:
    assert normalize_page_size(None) == 50
    assert normalize_page_size(200) == 200
    with pytest.raises(ValueError):
        normalize_page_size(0)
    with pytest.raises(ValueError):
        normalize_page_size(201)


def test_normalize_results_order_values() -> None:
    assert normalize_results_order(None) == "asc"
    assert normalize_results_order("desc") == "desc"
    with pytest.raises(ValueError):
        normalize_results_order("up")


def test_normalize_list_order_values() -> None:
    assert normalize_list_order(None) == "code_asc"
    assert normalize_list_order("name_desc") == "name_desc"
    with pytest.raises(ValueError):
        normalize_list_order("rank")


@pytest.mark.parametrize(
    ("data_type", "currency", "unit"),
    [
        ("stock", "CNY", "shares"),
        ("index", "CNY", "shares"),
        ("etf", "CNY", "shares"),
        ("fund", "CNY", "units"),
    ],
)
def test_default_currency_unit(data_type: str, currency: str, unit: str) -> None:
    resolved_currency, resolved_unit = default_currency_unit(data_type)
    assert resolved_currency == currency
    assert resolved_unit == unit


def test_default_currency_unit_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        default_currency_unit("crypto")
