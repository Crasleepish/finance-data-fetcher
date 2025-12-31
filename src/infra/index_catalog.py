from __future__ import annotations

from typing import Iterable

from sqlalchemy import Select, Table, select
from sqlalchemy.engine import Engine


def require_index_codes(engine: Engine, table: Table, codes: Iterable[str]) -> list[str]:
    """Ensure all codes exist in index_info table."""
    code_list = list(codes)
    if not code_list:
        return []
    existing = _load_index_codes(engine, table, code_list)
    missing = sorted(set(code_list) - existing)
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"index codes not found in index_info: {missing_csv}")
    return code_list


def _load_index_codes(engine: Engine, table: Table, codes: list[str]) -> set[str]:
    query: Select[tuple[str]] = select(table.c.index_code).where(table.c.index_code.in_(codes))
    with engine.begin() as conn:
        rows = conn.execute(query).fetchall()
    return {row[0] for row in rows}
