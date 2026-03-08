from __future__ import annotations

from datetime import date, datetime

ALLOWED_DATA_TYPES = {"stock", "fund", "index", "etf"}
RESULTS_ORDER = {"asc", "desc"}
LIST_ORDER = {"code_asc", "code_desc", "name_asc", "name_desc"}

DEFAULT_LIMIT = 5000
MAX_LIMIT = 20000
DEFAULT_PAGE = 1
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def validate_data_type(value: str) -> str:
    """Validate data_type against allowed values."""
    if value not in ALLOWED_DATA_TYPES:
        raise ValueError("data_type must be one of: stock, fund, index, etf")
    return value


def validate_asset_code(value: str) -> str:
    """Validate asset_code is non-empty."""
    if not value:
        raise ValueError("asset_code is required")
    return value


def parse_date(value: str) -> date:
    """Parse date in YYYY-MM-DD or YYYYMMDD format."""
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def parse_date_range(start_date: str, end_date: str) -> tuple[date, date]:
    """Parse and validate date range."""
    start = parse_date(start_date)
    end = parse_date(end_date)
    if start > end:
        raise ValueError("start_date must be <= end_date")
    return start, end


def normalize_limit(limit: int | None) -> int:
    """Validate limit or apply default."""
    if limit is None:
        return DEFAULT_LIMIT
    if limit <= 0:
        raise ValueError("limit must be positive")
    if limit > MAX_LIMIT:
        raise ValueError(f"limit must be <= {MAX_LIMIT}")
    return limit


def normalize_results_order(order: str | None) -> str:
    """Validate order for results endpoint."""
    if order is None:
        return "asc"
    if order not in RESULTS_ORDER:
        raise ValueError("order must be asc or desc")
    return order


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


def normalize_list_order(order: str | None) -> str:
    """Validate order for list endpoint."""
    if order is None:
        return "code_asc"
    if order not in LIST_ORDER:
        raise ValueError("order must be code_asc, code_desc, name_asc, or name_desc")
    return order
