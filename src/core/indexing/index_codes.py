from __future__ import annotations

from typing import Callable, Iterable, TypedDict


class IndexCodeMapping(TypedDict):
    """Index code mapping between stored code and API code."""

    index_code: str
    api_code: str


def parse_index_codes(raw: str) -> list[str]:
    """Parse comma-separated index codes into a stable list."""
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def build_code_mappings(
    codes: Iterable[str],
    converter: Callable[[str], str],
) -> list[IndexCodeMapping]:
    """Build index_code to api_code mappings."""
    return [{"index_code": code, "api_code": converter(code)} for code in codes]


def strip_suffix(code: str) -> str:
    """Remove suffix after the first dot, if any."""
    if "." in code:
        return code.split(".", 1)[0]
    return code


def strip_sge_suffix(code: str) -> str:
    """Remove .SGE suffix from SGE codes."""
    if code.endswith(".SGE"):
        return code[: -len(".SGE")]
    return code
