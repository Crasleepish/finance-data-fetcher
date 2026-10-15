"""One-off TuShare SW industry membership history fetch + atomic replace.

Fetches ``index_member_all`` (both ``is_new='Y'`` and ``is_new='N'``) plus
independent per-L1 shards, cross-checks the two complete raw row sets, then
atomically replaces the ``industry_component_history_tushare`` table.

Run from the repository root::

    uv run python extra/fetch_industry_component_history_tushare.py
    uv run python extra/fetch_industry_component_history_tushare.py --dry-run
    uv run python extra/fetch_industry_component_history_tushare.py --db-host 127.0.0.1

This script is intentionally standalone: it is not wired into the task/pipeline
system and does not touch any table other than its own target.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import math
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC = PROJECT_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import tushare as ts  # noqa: E402
from sqlalchemy import (  # noqa: E402
    Column,
    Date,
    Engine,
    MetaData,
    Table,
    Text,
    create_engine,
    func,
    insert,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Connection, make_url  # noqa: E402

from config.loader import load_config  # noqa: E402

logger = logging.getLogger("fetch_industry_component_history_tushare")

TABLE_NAME = "industry_component_history_tushare"

RAW_FIELDS: tuple[str, ...] = (
    "l1_code",
    "l1_name",
    "l2_code",
    "l2_name",
    "l3_code",
    "l3_name",
    "ts_code",
    "name",
    "in_date",
    "out_date",
    "is_new",
)
DATE_FIELDS = frozenset({"in_date", "out_date"})
REQUIRED_FIELDS = ("l1_code", "ts_code", "is_new")
TABLE_COLUMNS: tuple[str, ...] = ("row_hash",) + RAW_FIELDS

_NULL_TOKEN = "\x00NULL\x00"
_FIELD_SEP = "\x1f"

metadata = MetaData()
industry_component_history_tushare = Table(
    TABLE_NAME,
    metadata,
    Column("row_hash", Text, primary_key=True),
    Column("l1_code", Text),
    Column("l1_name", Text),
    Column("l2_code", Text),
    Column("l2_name", Text),
    Column("l3_code", Text),
    Column("l3_name", Text),
    Column("ts_code", Text),
    Column("name", Text),
    Column("in_date", Date),
    Column("out_date", Date),
    Column("is_new", Text),
)

CLASSIFY_SOURCES = ("SW2014", "SW2021")


class PaginationError(RuntimeError):
    """Raised when a paginated fetch loops or fails to make progress."""


class _RateLimiter:
    """Conservative fixed-interval rate limiter."""

    def __init__(self, min_interval_s: float, sleep: Callable[[float], None]) -> None:
        self._min_interval_s = max(0.0, min_interval_s)
        self._sleep = sleep
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self._min_interval_s:
            self._sleep(self._min_interval_s - elapsed)
        self._last = time.monotonic()


# ---------------------------------------------------------------------------
# Normalization / hashing
# ---------------------------------------------------------------------------


def parse_date(value: Any) -> date | None:
    """Parse a strict ``YYYYMMDD`` value into a date, mapping blanks to None."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text_value = str(value).strip()
    if not text_value or text_value.lower() in {"nan", "none", "nat", "<na>"}:
        return None
    if len(text_value) != 8 or not text_value.isdigit():
        raise ValueError("date value must be strict YYYYMMDD")
    return datetime.strptime(text_value, "%Y%m%d").date()


def _canonical(value: Any) -> str:
    if value is None:
        return _NULL_TOKEN
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value)


def compute_row_hash(record: Mapping[str, Any]) -> str:
    """Deterministic hash over the full 11-field raw row (null-aware)."""
    payload = _FIELD_SEP.join(_canonical(record.get(field)) for field in RAW_FIELDS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_record(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a raw API row into a typed record with a stable ``row_hash``."""
    for field in RAW_FIELDS:
        if field not in raw:
            raise ValueError(f"row is missing required column: {field}")

    record: dict[str, Any] = {}
    for field in RAW_FIELDS:
        if field in DATE_FIELDS:
            record[field] = parse_date(raw[field])
            continue
        value = raw[field]
        if value is None or (isinstance(value, float) and math.isnan(value)):
            record[field] = None
            continue
        text_value = str(value).strip()
        record[field] = text_value or None

    for field in REQUIRED_FIELDS:
        if record[field] is None:
            raise ValueError(f"row has null required field: {field}")
    if record["is_new"] not in ("Y", "N"):
        raise ValueError("is_new must be 'Y' or 'N'")

    record["row_hash"] = compute_row_hash(record)
    return record


def dedupe_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by the full 11-field row hash without silently dropping data."""
    by_hash: dict[str, dict[str, Any]] = {}
    for record in records:
        row_hash = str(record["row_hash"])
        existing = by_hash.get(row_hash)
        if existing is None:
            by_hash[row_hash] = dict(record)
        elif existing != dict(record):
            raise ValueError("row_hash collision with differing row content")
    return list(by_hash.values())


# ---------------------------------------------------------------------------
# Pagination / API
# ---------------------------------------------------------------------------


def paginate(
    fetch_page: Callable[[int, int], list[dict[str, Any]]],
    *,
    page_size: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    """Page until an empty page, rejecting repeats, stalls, or overrun."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    rows: list[dict[str, Any]] = []
    seen_pages: set[tuple[str, ...]] = set()
    offset = 0
    pages = 0
    while True:
        if pages >= max_pages:
            raise PaginationError(f"exceeded max_pages={max_pages}")
        page = fetch_page(offset, page_size)
        if not page:
            break
        signature = tuple(_row_signature(row) for row in page)
        if signature in seen_pages:
            raise PaginationError("repeated page detected; aborting to avoid data loss")
        seen_pages.add(signature)
        rows.extend(page)
        pages += 1
        # Advance by the number of rows actually returned so a server-side cap
        # smaller than the requested limit cannot silently skip rows.
        offset += len(page)
    return rows


def _row_signature(raw: Mapping[str, Any]) -> str:
    return _FIELD_SEP.join(str(raw.get(field, "")) for field in RAW_FIELDS)


def _api_fetch_page(
    pro: Any,
    *,
    is_new: str,
    l1_code: str | None,
    offset: int,
    limit: int,
) -> list[dict[str, Any]]:
    kwargs: dict[str, Any] = {
        "is_new": is_new,
        "offset": offset,
        "limit": limit,
        "fields": ",".join(RAW_FIELDS),
    }
    if l1_code:
        kwargs["l1_code"] = l1_code
    data = pro.index_member_all(**kwargs)
    if data is None:
        raise ValueError(
            "tushare index_member_all returned None (not an empty page); aborting fetch"
        )
    if isinstance(data, list):
        return [dict(row) for row in data]
    if getattr(data, "empty", False):
        return []
    return [dict(row) for row in data.to_dict("records")]


def _with_retry(
    fn: Callable[[], Any],
    *,
    attempts: int,
    base_delay_s: float,
    sleep: Callable[[float], None],
) -> Any:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - re-raised below with context
            last_error = exc
            if attempt < attempts - 1:
                delay = base_delay_s * (2**attempt)
                logger.warning(
                    "tushare request failed; backing off",
                    extra={"attempt": attempt + 1, "delay_s": round(delay, 3)},
                )
                sleep(delay)
    raise RuntimeError(
        f"tushare request failed after {attempts} attempts ({type(last_error).__name__})"
    ) from last_error


def _fetch_filtered(
    pro: Any,
    *,
    is_new: str,
    l1_code: str | None,
    page_size: int,
    max_pages: int,
    attempts: int,
    base_delay_s: float,
    sleep: Callable[[float], None],
    limiter: _RateLimiter,
) -> list[dict[str, Any]]:
    def fetch_page(offset: int, limit: int) -> list[dict[str, Any]]:
        def call() -> list[dict[str, Any]]:
            limiter.wait()
            return _api_fetch_page(pro, is_new=is_new, l1_code=l1_code, offset=offset, limit=limit)

        return _with_retry(call, attempts=attempts, base_delay_s=base_delay_s, sleep=sleep)

    return paginate(fetch_page, page_size=page_size, max_pages=max_pages)


def fetch_l1_codes(
    pro: Any,
    *,
    attempts: int,
    base_delay_s: float,
    sleep: Callable[[float], None],
    limiter: _RateLimiter,
) -> set[str]:
    """Collect L1 industry codes from SW classify sources (incl. retired codes)."""
    codes: set[str] = set()
    for source in CLASSIFY_SOURCES:

        def call(source: str = source) -> Any:
            limiter.wait()
            return pro.index_classify(src=source, level="L1", fields="index_code,level")

        data = _with_retry(call, attempts=attempts, base_delay_s=base_delay_s, sleep=sleep)
        if data is None:
            raise ValueError(f"index_classify returned None for source {source}; aborting fetch")
        rows = data if isinstance(data, list) else data.to_dict("records")
        for row in rows:
            if "index_code" not in row:
                raise ValueError(
                    f"index_classify row missing index_code for source {source}; aborting fetch"
                )
            code = str(row["index_code"] or "").strip()
            if code:
                codes.add(code)
    if not codes:
        raise ValueError("index_classify resolved no L1 industry codes from any source")
    return codes


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def collect_all(
    pro: Any,
    *,
    page_size: int,
    max_pages: int,
    attempts: int,
    base_delay_s: float,
    sleep: Callable[[float], None] = time.sleep,
    min_interval_s: float = 1.0,
) -> list[dict[str, Any]]:
    """Fetch global + per-L1 shards, cross-check them, and return deduped rows."""
    limiter = _RateLimiter(min_interval_s, sleep)

    global_records: list[dict[str, Any]] = []
    global_counts: dict[str, int] = {}
    for is_new in ("Y", "N"):
        raw_rows = _fetch_filtered(
            pro,
            is_new=is_new,
            l1_code=None,
            page_size=page_size,
            max_pages=max_pages,
            attempts=attempts,
            base_delay_s=base_delay_s,
            sleep=sleep,
            limiter=limiter,
        )
        for raw in raw_rows:
            record = normalize_record(raw)
            if record["is_new"] != is_new:
                raise ValueError("global index_member_all returned a mismatched is_new value")
            global_records.append(record)
        global_counts[is_new] = len(raw_rows)

    if not global_records:
        raise ValueError("global index_member_all returned an empty dataset")

    l1_codes = fetch_l1_codes(
        pro, attempts=attempts, base_delay_s=base_delay_s, sleep=sleep, limiter=limiter
    )
    l1_codes |= {str(record["l1_code"]) for record in global_records}
    if not l1_codes:
        raise ValueError("no L1 industry codes resolved from classify or global rows")

    shard_records: list[dict[str, Any]] = []
    shard_counts: dict[str, int] = {"Y": 0, "N": 0}
    for l1_code in sorted(l1_codes):
        for is_new in ("Y", "N"):
            raw_rows = _fetch_filtered(
                pro,
                is_new=is_new,
                l1_code=l1_code,
                page_size=page_size,
                max_pages=max_pages,
                attempts=attempts,
                base_delay_s=base_delay_s,
                sleep=sleep,
                limiter=limiter,
            )
            for raw in raw_rows:
                record = normalize_record(raw)
                if record["l1_code"] != l1_code:
                    raise ValueError("per-L1 shard returned a row outside the filter")
                if record["is_new"] != is_new:
                    raise ValueError("per-L1 shard returned a mismatched is_new value")
                shard_records.append(record)
            shard_counts[is_new] += len(raw_rows)

    global_hashes = {str(record["row_hash"]) for record in global_records}
    shard_hashes = {str(record["row_hash"]) for record in shard_records}
    if global_hashes != shard_hashes:
        raise ValueError(
            "global rows and per-L1 shard rows differ "
            f"(global_only={len(global_hashes - shard_hashes)}, "
            f"shard_only={len(shard_hashes - global_hashes)})"
        )

    logger.info(
        "fetch verified: rows=%d (global Y=%d N=%d; shard Y=%d N=%d), l1_codes=%d, "
        "global_rows=%d, shard_rows=%d",
        len(global_records),
        global_counts.get("Y", 0),
        global_counts.get("N", 0),
        shard_counts.get("Y", 0),
        shard_counts.get("N", 0),
        len(l1_codes),
        len(global_records),
        len(shard_records),
    )
    return dedupe_records(global_records)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _type_family(column_type: Any) -> str:
    """Collapse a SQLAlchemy type into a coarse family for compatibility checks."""
    affinity = getattr(column_type, "_type_affinity", None)
    name = affinity.__name__ if affinity is not None else type(column_type).__name__
    if name == "String":
        # Text and Varchar share the String affinity and are interchangeable here.
        return "string"
    return name.lower()


def _assert_table_compatible(connection: Connection) -> None:
    """Reject an existing target table whose schema differs from the expected one."""
    inspector = inspect(connection)
    if not inspector.has_table(TABLE_NAME):
        return

    columns = inspector.get_columns(TABLE_NAME)
    existing_names = {column["name"] for column in columns}
    if existing_names != set(TABLE_COLUMNS):
        raise RuntimeError("existing target table schema is incompatible; refusing to modify it")

    expected_families = {
        name: _type_family(industry_component_history_tushare.c[name].type)
        for name in TABLE_COLUMNS
    }
    for column in columns:
        expected = expected_families[column["name"]]
        actual = _type_family(column["type"])
        if expected == "date":
            if actual != "date":
                raise RuntimeError(
                    "existing target table schema is incompatible; refusing to modify it"
                )
        elif actual != "string":
            raise RuntimeError(
                "existing target table schema is incompatible; refusing to modify it"
            )

    primary_key = inspector.get_pk_constraint(TABLE_NAME)
    pk_columns = list(primary_key.get("constrained_columns") or [])
    if pk_columns != ["row_hash"]:
        raise RuntimeError("existing target table schema is incompatible; refusing to modify it")


def _chunked(records: Sequence[Mapping[str, Any]], size: int) -> Iterable[list[Mapping[str, Any]]]:
    for start in range(0, len(records), size):
        yield list(records[start : start + size])


def _verify_published(
    connection: Any,
    table: Table,
    records: Sequence[Mapping[str, Any]],
) -> None:
    expected = len(records)
    actual = connection.execute(select(func.count()).select_from(table)).scalar_one()
    if actual != expected:
        raise RuntimeError(f"published row count mismatch: expected {expected}, got {actual}")
    stored = {str(row[0]) for row in connection.execute(select(table.c.row_hash))}
    expected_hashes = {str(record["row_hash"]) for record in records}
    if stored != expected_hashes:
        raise RuntimeError("published row set does not match fetched records")


def publish(
    engine: Engine,
    records: Sequence[Mapping[str, Any]],
    *,
    batch_size: int = 1000,
) -> int:
    """Atomically replace the target table with the provided records.

    Table creation, lock acquisition, replace and verification all run on the
    same connection inside one transaction so a failure cannot leave a
    half-written or empty table behind on databases with transactional DDL.
    """
    if not records:
        raise ValueError("refusing to publish an empty dataset")

    table = industry_component_history_tushare
    with engine.begin() as connection:
        _assert_table_compatible(connection)
        table.create(connection, checkfirst=True)
        if connection.dialect.name == "postgresql":
            preparer = connection.dialect.identifier_preparer
            connection.execute(
                text(f"LOCK TABLE {preparer.quote(TABLE_NAME)} IN SHARE ROW EXCLUSIVE MODE")
            )
        connection.execute(table.delete())
        for batch in _chunked(records, batch_size):
            connection.execute(insert(table), batch)
        _verify_published(connection, table, records)

    logger.info("published rows=%d", len(records))
    return len(records)


def build_engine(config: Any, host: str | None, port: int | None) -> Engine:
    """Build an engine, overriding host/port in memory only."""
    url = make_url(config.database.url)
    overrides: dict[str, Any] = {}
    if host:
        overrides["host"] = host
    if port:
        overrides["port"] = port
    if overrides:
        url = url.set(**overrides)
    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="fetch and verify only")
    parser.add_argument("--db-host", default=None, help="override DB host in memory")
    parser.add_argument("--db-port", type=int, default=None, help="override DB port in memory")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--insert-batch-size", type=int, default=1000)
    parser.add_argument("--min-interval", type=float, default=1.0)
    parser.add_argument("--retry-attempts", type=int, default=4)
    parser.add_argument("--retry-base-delay", type=float, default=1.0)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        config = load_config()
        token = config.tushare.token_private
        if not token:
            logger.error("missing tushare token_private in configuration")
            return 1

        pro = ts.pro_api(token)
        records = collect_all(
            pro,
            page_size=args.page_size,
            max_pages=args.max_pages,
            attempts=args.retry_attempts,
            base_delay_s=args.retry_base_delay,
            min_interval_s=args.min_interval,
        )

        if args.dry_run:
            logger.info("dry-run complete: rows=%d (database write skipped)", len(records))
            return 0

        engine = build_engine(config, args.db_host, args.db_port)
        try:
            publish(engine, records, batch_size=args.insert_batch_size)
        finally:
            engine.dispose()
    except Exception as exc:  # noqa: BLE001 - never surface chained/token-bearing details
        logger.error(
            "aborting after %s; see earlier log lines for the failing stage",
            type(exc).__name__,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
