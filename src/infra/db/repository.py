from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from sqlalchemy import Engine, Table, insert
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import CursorResult
from sqlalchemy.sql import Executable

from infra.db.engine import transaction


class UpsertStrategy(Protocol):
    def build_upsert(
        self,
        table: Table,
        records: Sequence[Mapping[str, Any]],
        unique_keys: Sequence[str],
    ) -> Executable: ...


@dataclass(frozen=True)
class PostgresUpsertStrategy:
    def build_upsert(
        self,
        table: Table,
        records: Sequence[Mapping[str, Any]],
        unique_keys: Sequence[str],
    ) -> Executable:
        if not unique_keys:
            raise ValueError("unique_keys is required for upsert")

        stmt = postgresql.insert(table).values(list(records))
        update_columns = {
            column.name: getattr(stmt.excluded, column.name)
            for column in table.columns
            if column.name not in unique_keys
        }
        return stmt.on_conflict_do_update(index_elements=list(unique_keys), set_=update_columns)


@dataclass(frozen=True)
class Repository:
    engine: Engine
    table: Table

    def insert_batch(self, records: Sequence[Mapping[str, Any]]) -> int:
        if not records:
            return 0

        stmt = insert(self.table).values(list(records))
        return self._execute_write(stmt)

    def upsert_batch(
        self,
        records: Sequence[Mapping[str, Any]],
        unique_keys: Sequence[str],
        strategy: UpsertStrategy | None = None,
    ) -> int:
        if not records:
            return 0

        resolved_strategy = strategy or PostgresUpsertStrategy()
        stmt = resolved_strategy.build_upsert(self.table, records, unique_keys)
        return self._execute_write(stmt)

    def _execute_write(self, stmt: Executable) -> int:
        with transaction(self.engine) as connection:
            result = connection.execute(stmt)
            return _rowcount(result)


def _rowcount(result: CursorResult[Any]) -> int:
    return result.rowcount
