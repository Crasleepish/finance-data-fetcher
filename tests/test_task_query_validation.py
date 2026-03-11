from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.task_query.validation import (
    normalize_page,
    normalize_page_size,
    parse_datetime,
    parse_datetime_range,
)


def test_parse_datetime_accepts_iso_with_timezone() -> None:
    parsed = parse_datetime("2024-01-02T10:11:12+08:00")
    assert parsed == datetime(2024, 1, 2, 2, 11, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("value", ["2024-01-02", "2024-01-02T10:11:12"])
def test_parse_datetime_requires_timezone(value: str) -> None:
    with pytest.raises(ValueError):
        parse_datetime(value)


@pytest.mark.parametrize("value", ["not-a-date", "2024-13-01T00:00:00Z"])
def test_parse_datetime_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_datetime(value)


def test_parse_datetime_range_validates_order() -> None:
    start, end = parse_datetime_range(
        "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z"
    )
    assert start == datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError):
        parse_datetime_range("2024-01-04T00:00:00Z", "2024-01-03T00:00:00Z")


def test_normalize_page_defaults_and_bounds() -> None:
    assert normalize_page(None) == 1
    assert normalize_page(2) == 2
    with pytest.raises(ValueError):
        normalize_page(0)


def test_normalize_page_size_defaults_and_caps() -> None:
    assert normalize_page_size(None) == 50
    assert normalize_page_size(200) == 200
    with pytest.raises(ValueError):
        normalize_page_size(0)
    with pytest.raises(ValueError):
        normalize_page_size(201)
