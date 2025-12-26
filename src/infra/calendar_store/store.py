from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence, cast

from sqlalchemy import Engine, Table, func, select
from sqlalchemy.dialects import postgresql

from infra.db.engine import transaction
from infra.db.tables import trade_calendar


@dataclass(frozen=True)
class CalendarStore:
    """SQL-backed store for trade calendar dates."""
    engine: Engine
    table: Table = trade_calendar

    def get_bounds(self) -> tuple[date, date] | None:
        """Return min/max trade dates or None if empty."""
        stmt = select(func.min(self.table.c.date), func.max(self.table.c.date))
        with transaction(self.engine) as connection:
            min_date, max_date = cast(
                tuple[date | None, date | None], connection.execute(stmt).one()
            )
        if min_date is None or max_date is None:
            return None
        return min_date, max_date

    def get_trade_days(self, start: date, end: date) -> list[date]:
        """Return sorted trade days within range."""
        stmt = (
            select(self.table.c.date)
            .where(self.table.c.date >= start, self.table.c.date <= end)
            .order_by(self.table.c.date)
        )
        with transaction(self.engine) as connection:
            rows = connection.execute(stmt).all()
        return [row.date for row in rows]

    def is_trade_day(self, day: date) -> bool:
        """Check if a date exists in the trade calendar table."""
        stmt = select(self.table.c.date).where(self.table.c.date == day)
        with transaction(self.engine) as connection:
            row = connection.execute(stmt).first()
        return row is not None

    def prev_trade_day(self, day: date) -> date | None:
        """Return last trade day before the given date."""
        stmt = select(func.max(self.table.c.date)).where(self.table.c.date < day)
        with transaction(self.engine) as connection:
            value = connection.execute(stmt).scalar()
        return cast(date | None, value)

    def next_trade_day(self, day: date) -> date | None:
        """Return next trade day after the given date."""
        stmt = select(func.min(self.table.c.date)).where(self.table.c.date > day)
        with transaction(self.engine) as connection:
            value = connection.execute(stmt).scalar()
        return cast(date | None, value)

    def insert_trade_days(self, days: Sequence[date]) -> int:
        """Insert trade days with conflict ignore; return inserted count."""
        if not days:
            return 0
        records = [{"date": day} for day in days]
        stmt = (
            postgresql.insert(self.table)
            .values(records)
            .on_conflict_do_nothing(index_elements=[self.table.c.date])
        )
        with transaction(self.engine) as connection:
            result = connection.execute(stmt)
        return int(result.rowcount or 0)
