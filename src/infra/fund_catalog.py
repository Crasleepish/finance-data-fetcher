from __future__ import annotations

from typing import Iterable

from sqlalchemy import Select, Table, or_, select
from sqlalchemy.engine import Engine


def load_index_fund_codes(engine: Engine, table: Table) -> list[str]:
    """Load index fund codes from fund_info based on invest_type/fund_type."""
    stmt = select(table.c.fund_code).where(
        or_(
            table.c.invest_type.in_(["被动指数型", "增强指数型"]),
            table.c.fund_type == "商品型",
        )
    )
    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()
    return sorted({row[0] for row in rows if row[0]})


def validate_money_fund_codes(engine: Engine, table: Table, codes: Iterable[str]) -> list[str]:
    """Validate configured money fund codes are present and marked as 货币型."""
    code_list = _unique_codes(codes)
    if not code_list:
        return []
    stmt: Select[tuple[str, str | None]] = select(table.c.fund_code, table.c.invest_type).where(
        table.c.fund_code.in_(code_list)
    )
    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()
    found = {row[0]: row[1] for row in rows}
    missing = [code for code in code_list if code not in found]
    invalid = [code for code, invest_type in found.items() if invest_type != "货币型"]
    if missing or invalid:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if invalid:
            details.append(f"not money type: {', '.join(sorted(invalid))}")
        raise ValueError(f"money fund codes validation failed ({'; '.join(details)})")
    return code_list


def parse_fund_codes(raw: str) -> list[str]:
    """Parse comma-separated fund codes into a stable list."""
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _unique_codes(codes: Iterable[str]) -> list[str]:
    seen = set()
    ordered: list[str] = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered
