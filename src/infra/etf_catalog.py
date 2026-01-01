from __future__ import annotations

from sqlalchemy import Table, select
from sqlalchemy.engine import Engine


def load_etf_codes(engine: Engine, table: Table) -> list[str]:
    """Load ETF codes from etf_info table."""
    stmt = select(table.c.etf_code)
    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()
    return sorted({row[0] for row in rows if row[0]})
