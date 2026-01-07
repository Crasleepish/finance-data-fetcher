from __future__ import annotations

from sqlalchemy import Select, Table, select
from sqlalchemy.engine import Engine


def load_etf_codes(engine: Engine, table: Table, name_like: str | None = None) -> list[str]:
    """Load ETF codes, optionally filtered by etf_name pattern."""
    query: Select[tuple[str]] = select(table.c.etf_code)
    if name_like:
        query = query.where(table.c.etf_name.like(name_like))
    with engine.begin() as conn:
        rows = conn.execute(query).fetchall()
    return [row[0] for row in rows if row[0]]
