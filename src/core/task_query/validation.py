from __future__ import annotations

from datetime import datetime, timezone

DEFAULT_PAGE = 1
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def parse_datetime(value: str) -> datetime:
    """Parse ISO 8601 datetime with timezone."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("datetime must be ISO 8601 with timezone") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("datetime must include timezone")
    return parsed.astimezone(timezone.utc)


def parse_datetime_range(
    start: str | None, end: str | None
) -> tuple[datetime | None, datetime | None]:
    """Parse optional datetime range ensuring start <= end."""
    if start is None and end is None:
        return None, None
    parsed_start = parse_datetime(start) if start is not None else None
    parsed_end = parse_datetime(end) if end is not None else None
    if parsed_start and parsed_end and parsed_start > parsed_end:
        raise ValueError("start must be <= end")
    return parsed_start, parsed_end


def normalize_page(page: int | None) -> int:
    """Validate page or apply default."""
    if page is None:
        return DEFAULT_PAGE
    if page <= 0:
        raise ValueError("page must be positive")
    return page


def normalize_page_size(page_size: int | None) -> int:
    """Validate page_size or apply default."""
    if page_size is None:
        return DEFAULT_PAGE_SIZE
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    if page_size > MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be <= {MAX_PAGE_SIZE}")
    return page_size
