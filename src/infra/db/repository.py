from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from sqlalchemy import Engine, Table, and_, column, delete, insert, values
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import Executable

from infra.db.engine import transaction

logger = logging.getLogger(__name__)


class UpsertStrategy(Protocol):
    """Strategy interface for upsert statements."""

    def build_upsert(
        self,
        table: Table,
        records: Sequence[Mapping[str, Any]],
        unique_keys: Sequence[str],
    ) -> Executable: ...


@dataclass(frozen=True)
class PostgresUpsertStrategy:
    """PostgreSQL upsert strategy using ON CONFLICT DO UPDATE."""

    def build_upsert(
        self,
        table: Table,
        records: Sequence[Mapping[str, Any]],
        unique_keys: Sequence[str],
    ) -> Executable:
        if not unique_keys:
            raise ValueError("unique_keys is required for upsert")

        stmt = postgresql.insert(table).values(list(records))
        record_columns = {key for record in records for key in record}
        update_columns = {
            column.name: getattr(stmt.excluded, column.name)
            for column in table.columns
            if column.name not in unique_keys and column.name in record_columns
        }
        return stmt.on_conflict_do_update(index_elements=list(unique_keys), set_=update_columns)


@dataclass(frozen=True)
class Repository:
    """Repository helper for batch insert/upsert with explicit transactions."""

    engine: Engine
    table: Table

    def insert_batch(self, records: Sequence[Mapping[str, Any]]) -> int:
        """Insert a batch of records and return affected row count."""
        if not records:
            return 0

        logger.info("persist start", extra={"batch_size": len(records)})
        started_at = time.monotonic()
        stmt = insert(self.table).values(list(records))
        affected = self._execute_write(stmt, started_at)
        logger.info(
            "persist commit",
            extra={
                "inserted": affected,
                "updated": 0,
                "skipped": 0,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            },
        )
        return affected

    def upsert_batch(
        self,
        records: Sequence[Mapping[str, Any]],
        unique_keys: Sequence[str],
        strategy: UpsertStrategy | None = None,
    ) -> int:
        """Upsert a batch of records using the provided strategy."""
        if not records:
            return 0

        logger.info("persist start", extra={"batch_size": len(records)})
        started_at = time.monotonic()
        resolved_strategy = strategy or PostgresUpsertStrategy()
        stmt = resolved_strategy.build_upsert(self.table, records, unique_keys)
        affected = self._execute_write(stmt, started_at)
        logger.info(
            "persist commit",
            extra={
                "inserted": 0,
                "updated": affected,
                "skipped": 0,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            },
        )
        return affected

    def replace_all(self, records: Sequence[Mapping[str, Any]]) -> int:
        """Replace table contents with the provided records in one transaction."""
        if not records:
            return 0

        logger.info("persist start", extra={"batch_size": len(records)})
        started_at = time.monotonic()
        try:
            with transaction(self.engine) as connection:
                connection.execute(delete(self.table))
                result = connection.execute(insert(self.table).values(list(records)))
            affected = _rowcount(result)
        except DBAPIError as exc:
            sql_state = getattr(getattr(exc, "orig", None), "pgcode", None)
            logger.error(
                "database write failed",
                extra={
                    "sql_state": sql_state,
                    "deadlock": sql_state == "40P01",
                    "duration_ms": int((time.monotonic() - started_at) * 1000),
                },
            )
            raise
        logger.info(
            "persist commit",
            extra={
                "inserted": affected,
                "updated": 0,
                "skipped": 0,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            },
        )
        return affected

    def update_batch(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        key_columns: Sequence[str],
        update_columns: Sequence[str],
        batch_size: int = 5000,
    ) -> int:
        """Update matching rows only using PostgreSQL VALUES batches."""
        if not records:
            return 0
        if not key_columns:
            raise ValueError("key_columns is required for update")
        if not update_columns:
            raise ValueError("update_columns is required for update")

        deduped_records = _dedupe_records(records, key_columns)
        logger.info("persist start", extra={"batch_size": len(deduped_records)})
        started_at = time.monotonic()
        affected = 0
        try:
            with transaction(self.engine) as connection:
                for start in range(0, len(deduped_records), batch_size):
                    batch = list(deduped_records[start : start + batch_size])
                    stmt = _build_batch_update_stmt(
                        self.table,
                        batch,
                        key_columns=key_columns,
                        update_columns=update_columns,
                    )
                    affected += _rowcount(connection.execute(stmt))
        except DBAPIError as exc:
            sql_state = getattr(getattr(exc, "orig", None), "pgcode", None)
            logger.error(
                "database write failed",
                extra={
                    "sql_state": sql_state,
                    "deadlock": sql_state == "40P01",
                    "duration_ms": int((time.monotonic() - started_at) * 1000),
                },
            )
            raise
        logger.info(
            "persist commit",
            extra={
                "inserted": 0,
                "updated": affected,
                "skipped": 0,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            },
        )
        return affected

    def _execute_write(self, stmt: Executable, started_at: float) -> int:
        """Execute a write statement within a transaction."""
        try:
            with transaction(self.engine) as connection:
                result = connection.execute(stmt)
                return _rowcount(result)
        except DBAPIError as exc:
            sql_state = getattr(getattr(exc, "orig", None), "pgcode", None)
            logger.error(
                "database write failed",
                extra={
                    "sql_state": sql_state,
                    "deadlock": sql_state == "40P01",
                    "duration_ms": int((time.monotonic() - started_at) * 1000),
                },
            )
            raise


def _build_batch_update_stmt(
    table: Table,
    records: Sequence[Mapping[str, Any]],
    *,
    key_columns: Sequence[str],
    update_columns: Sequence[str],
) -> Executable:
    names = list(key_columns) + list(update_columns)
    rows = [tuple(record[name] for name in names) for record in records]
    value_table = values(*(column(name) for name in names), name="incoming").data(rows).alias()
    conditions = [table.c[name] == value_table.c[name] for name in key_columns]
    assignments = {name: value_table.c[name] for name in update_columns}
    return table.update().where(and_(*conditions)).values(assignments)


def _dedupe_records(
    records: Sequence[Mapping[str, Any]],
    key_columns: Sequence[str],
) -> list[Mapping[str, Any]]:
    deduped: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    for record in records:
        key = tuple(record[name] for name in key_columns)
        deduped[key] = record
    return list(deduped.values())


def _rowcount(result: CursorResult[Any]) -> int:
    """Return rowcount as int for write statements."""
    return result.rowcount
