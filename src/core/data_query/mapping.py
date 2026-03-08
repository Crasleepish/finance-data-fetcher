from __future__ import annotations

DATA_TYPE_CURRENCY_UNIT = {
    "stock": ("CNY", "shares"),
    "index": ("CNY", "shares"),
    "etf": ("CNY", "shares"),
    "fund": ("CNY", "units"),
}


def default_currency_unit(data_type: str) -> tuple[str, str]:
    """Return default currency/unit for a data_type."""
    mapping = DATA_TYPE_CURRENCY_UNIT.get(data_type)
    if mapping is None:
        raise ValueError("unsupported data_type")
    return mapping
