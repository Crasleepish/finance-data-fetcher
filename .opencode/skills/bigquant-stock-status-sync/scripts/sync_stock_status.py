#!/usr/bin/env python3
"""Sync BigQuant ``cn_stock_status`` Parquet exports into stock_hist_unadj.

Subcommands:

* ``verify``    -- source-only manifest/Parquet validation (no database access)
* ``preflight`` -- validate + stage into a TEMP table, report join coverage (no target writes)
* ``apply``     -- atomic overwrite of stock_hist_unadj.is_st / is_suspend

Write scope is exactly those two columns: no inserts, no deletes, no schema changes.
Safe to run --help without the project dependencies installed.
"""

from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

MANIFEST_NAME = "manifest.csv"
BATCH_ROWS = 262_144
REQUIRED_COLUMNS = ("date", "instrument", "is_risk_warning", "suspended")
FILE_RE = re.compile(r"^cn_stock_status_(\d{4})(\d{2})(\d{2})_(\d{4})(\d{2})(\d{2})\.parquet$")
CODE_RE = r"^[0-9]{6}\.(SH|SZ|BJ)$"
COPY_SQL = "COPY stage_stock_status (stock_code, trade_date, is_st, is_suspend) FROM STDIN"

MARK_BEGIN = "SYNC_RESULT_JSON_BEGIN"
MARK_END = "SYNC_RESULT_JSON_END"

INVARIANTS_SQL = """
SELECT
 (SELECT count(*) FROM stock_hist_unadj) AS target_rows,
 (SELECT count(*) FROM stage_stock_status s
    JOIN stock_hist_unadj t ON t.stock_code = s.stock_code AND t.date = s.trade_date)
    AS matched,
 (SELECT count(*) FROM stage_stock_status s
    JOIN stock_hist_unadj t ON t.stock_code = s.stock_code AND t.date = s.trade_date
    WHERE t.is_st IS DISTINCT FROM s.is_st
       OR t.is_suspend IS DISTINCT FROM s.is_suspend) AS matched_mismatch,
 (SELECT count(*) FROM stock_hist_unadj t
    WHERE NOT EXISTS (SELECT 1 FROM stage_stock_status s
                      WHERE s.stock_code = t.stock_code AND s.trade_date = t.date)
      AND (t.is_st IS NOT NULL OR t.is_suspend IS DISTINCT FROM 'N')) AS unmatched_nondefault,
 (SELECT count(*) FROM stock_hist_unadj
    WHERE is_st IS NOT NULL AND is_st NOT IN (0, 1)) AS is_st_bad,
 (SELECT count(*) FROM stock_hist_unadj
    WHERE is_suspend IS NULL OR is_suspend NOT IN ('N', 'Y')) AS suspend_bad,
 (SELECT count(*) FROM stage_stock_status s
    WHERE NOT EXISTS (SELECT 1 FROM stock_hist_unadj t
                      WHERE t.stock_code = s.stock_code AND t.date = s.trade_date))
    AS src_absent,
 (SELECT count(*) FROM stock_hist_unadj t
    WHERE NOT EXISTS (SELECT 1 FROM stage_stock_status s
                      WHERE s.stock_code = t.stock_code AND s.trade_date = t.date))
    AS tgt_unmatched
"""

DISTRIBUTIONS_SQL = """
SELECT count(*) FILTER (WHERE is_st IS NULL),
       count(*) FILTER (WHERE is_st = 0),
       count(*) FILTER (WHERE is_st = 1),
       count(*) FILTER (WHERE is_suspend = 'N'),
       count(*) FILTER (WHERE is_suspend = 'Y')
FROM stock_hist_unadj
"""

UPDATE_MATCHED_SQL = """
UPDATE stock_hist_unadj t
SET is_st = s.is_st,
    is_suspend = s.is_suspend
FROM stage_stock_status s
WHERE t.stock_code = s.stock_code
  AND t.date = s.trade_date
  AND (t.is_st IS DISTINCT FROM s.is_st OR t.is_suspend IS DISTINCT FROM s.is_suspend)
"""

UPDATE_UNMATCHED_SQL = """
UPDATE stock_hist_unadj t
SET is_st = NULL, is_suspend = 'N'
WHERE (t.is_st IS NOT NULL OR t.is_suspend IS DISTINCT FROM 'N')
  AND NOT EXISTS (
      SELECT 1 FROM stage_stock_status s
      WHERE s.stock_code = t.stock_code AND s.trade_date = t.date
  )
"""

STAGE_DDL = """
CREATE TEMP TABLE stage_stock_status (
    stock_code text NOT NULL,
    trade_date date NOT NULL,
    is_st smallint NOT NULL CHECK (is_st IN (0, 1)),
    is_suspend char(1) NOT NULL CHECK (is_suspend IN ('N', 'Y'))
) ON COMMIT PRESERVE ROWS
"""


class SyncError(Exception):
    """Fatal validation or execution failure."""


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def emit(result: dict[str, Any]) -> None:
    print(MARK_BEGIN)
    print(json.dumps(result, indent=2, default=str))
    print(MARK_END)


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def _find_project_root(start: Path) -> Path | None:
    for candidate in [start, *start.parents]:
        if (candidate / "pyproject.toml").exists() and (candidate / "config").is_dir():
            return candidate
    return None


def resolve_config_path() -> Path:
    explicit = os.environ.get("APP_CONFIG_PATH")
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise SyncError("APP_CONFIG_PATH points to a missing file")
        return path
    root = _find_project_root(Path(__file__).resolve())
    if root is not None:
        return root / "config" / "app.yaml"
    root = _find_project_root(Path.cwd().resolve())
    if root is not None:
        return root / "config" / "app.yaml"
    raise SyncError("cannot locate config/app.yaml; set APP_CONFIG_PATH")


def load_dsn() -> str:
    """Return a libpq-compatible DSN. The value is never printed."""
    url: str | None = None
    try:
        from config.loader import load_config  # type: ignore[import-not-found]

        url = load_config().database.url
    except Exception:
        pass
    if not url:
        env_url = os.environ.get("APP_DB_URL")
        if env_url:
            url = env_url
    if not url:
        import yaml

        path = resolve_config_path()
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise SyncError("config file must contain a mapping")
        database = raw.get("database") or {}
        url = database.get("url")
    if not url:
        raise SyncError("database.url is not configured")
    return re.sub(r"^postgresql\+\w+://", "postgresql://", url)


def connect(app_name: str) -> Any:
    import psycopg2

    return psycopg2.connect(load_dsn(), application_name=app_name)


# ---------------------------------------------------------------------------
# source validation
# ---------------------------------------------------------------------------


def parse_file_period(path: Path) -> tuple[date, date]:
    match = FILE_RE.match(path.name)
    if match is None:
        raise SyncError(f"{path.name}: filename does not match cn_stock_status_*.parquet")
    g = match.groups()
    return date(int(g[0]), int(g[1]), int(g[2])), date(int(g[3]), int(g[4]), int(g[5]))


def read_manifest(source_dir: Path, manifest_name: str) -> list[dict[str, Any]]:
    path = source_dir / manifest_name
    if not path.exists():
        raise SyncError(f"manifest not found: {path.name} (pass --manifest to override)")
    entries: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            try:
                entry = {
                    "file": raw["file"],
                    "start_date": raw["start_date"],
                    "end_date": raw["end_date"],
                    "rows": int(raw["rows"]),
                    "status": (raw.get("status") or "").strip(),
                }
            except (KeyError, TypeError, ValueError) as exc:
                raise SyncError(f"invalid manifest row {raw!r}") from exc
            entries.append(entry)
    if not entries:
        raise SyncError("manifest contains no entries")
    return entries


def check_manifest(entries: list[dict[str, Any]]) -> None:
    seen_files: set[str] = set()
    ranges: list[tuple[date, date, str]] = []
    for entry in entries:
        name = entry["file"]
        if name in seen_files:
            raise SyncError(f"manifest lists file twice: {name}")
        seen_files.add(name)
        if FILE_RE.match(name) is None:
            raise SyncError(f"manifest filename does not match pattern: {name}")
        start = date.fromisoformat(entry["start_date"])
        end = date.fromisoformat(entry["end_date"])
        if start > end:
            raise SyncError(f"{name}: start_date after end_date")
        file_start, file_end = parse_file_period(Path(name))
        if (file_start, file_end) != (start, end):
            raise SyncError(f"{name}: filename range != manifest range {start}..{end}")
        if entry["rows"] < 0:
            raise SyncError(f"{name}: negative manifest rows")
        if entry["rows"] > 0:
            ranges.append((start, end, name))
    ranges.sort()
    for (s1, e1, n1), (s2, e2, _) in zip(ranges, ranges[1:]):
        if s2 <= e1:
            raise SyncError(f"overlapping periods: {n1} ({s1}..{e1}) and next ({s2}..{e2})")


def check_date_column(arr: Any, ctx: str) -> Any:
    import pyarrow as pa
    import pyarrow.compute as pc

    if arr.null_count:
        raise SyncError(f"{ctx}: date nulls={arr.null_count}")
    if pa.types.is_date(arr.type):
        return arr
    if pa.types.is_timestamp(arr.type):
        if arr.type.tz is not None:
            raise SyncError(f"{ctx}: date column carries timezone {arr.type.tz}")
        midnight = pc.cast(pc.cast(arr, pa.date32()), pa.timestamp(arr.type.unit))
        bad = pc.sum(pc.cast(pc.not_equal(midnight, arr), pa.int64())).as_py()
        if bad:
            raise SyncError(f"{ctx}: non-midnight timestamps n={bad}")
        return pc.cast(arr, pa.date32())
    raise SyncError(f"{ctx}: unsupported date type {arr.type}")


def check_codes(arr: Any, ctx: str) -> None:
    import pyarrow.compute as pc

    if arr.null_count:
        raise SyncError(f"{ctx}: instrument nulls={arr.null_count}")
    mask = pc.match_substring_regex(arr, CODE_RE)
    if not pc.all(mask).as_py():
        bad = pc.filter(arr, pc.invert(mask)).to_pylist()[:5]
        raise SyncError(f"{ctx}: instrument regex violations examples={bad}")


def check_binary(arr: Any, name: str, ctx: str) -> None:
    import pyarrow as pa
    import pyarrow.compute as pc

    if arr.null_count:
        raise SyncError(f"{ctx}: {name} nulls={arr.null_count}")
    if pa.types.is_boolean(arr.type):
        return
    if pa.types.is_integer(arr.type) or pa.types.is_floating(arr.type):
        valid = pc.is_in(arr, value_set=pa.array([0, 1], type=arr.type))
        if not pc.all(valid).as_py():
            bad = pc.unique(pc.filter(arr, pc.invert(valid))).to_pylist()[:10]
            raise SyncError(f"{ctx}: {name} out-of-domain values {bad}")
        return
    raise SyncError(f"{ctx}: {name} unsupported type {arr.type}")


def validate_batch(batch: Any, ctx: str) -> Any:
    """Validate one batch; return the date column cast to date32."""
    check_codes(batch.column("instrument"), ctx)
    check_binary(batch.column("is_risk_warning"), "is_risk_warning", ctx)
    check_binary(batch.column("suspended"), "suspended", ctx)
    return check_date_column(batch.column("date"), ctx)


def as_int_list(arr: Any) -> list[int]:
    import pyarrow as pa

    if pa.types.is_boolean(arr.type):
        return [1 if v else 0 for v in arr.to_pylist()]
    return [int(v) for v in arr.to_pylist()]


def copy_batch(cur: Any, batch: Any, ctx: str) -> int:
    dates = validate_batch(batch, ctx)
    codes = batch.column("instrument").to_pylist()
    sts = as_int_list(batch.column("is_risk_warning"))
    suspend = as_int_list(batch.column("suspended"))
    buf = io.StringIO()
    buf.write(
        "".join(
            f"{code}\t{dt.isoformat()}\t{st}\t{'Y' if sus else 'N'}\n"
            for code, dt, st, sus in zip(codes, dates.to_pylist(), sts, suspend)
        )
    )
    buf.seek(0)
    cur.copy_expert(COPY_SQL, buf)
    return batch.num_rows


def check_no_extra_files(source_dir: Path, entries: list[dict[str, Any]]) -> None:
    files_on_disk = {Path(p).name for p in glob.glob(str(source_dir / "cn_stock_status_*.parquet"))}
    unknown = sorted(files_on_disk - {e["file"] for e in entries})
    if unknown:
        raise SyncError(f"parquet files not present in manifest: {unknown}")


def batch_date_bounds(dates: Any, ctx: str, lower: date, upper: date) -> tuple[date, date]:
    """Return the batch min/max date and reject any row outside [lower, upper]."""
    import pyarrow.compute as pc

    mm = pc.min_max(dates).as_py()
    if mm["min"] is None or mm["max"] is None:
        raise SyncError(f"{ctx}: no dates in batch")
    lo, hi = mm["min"], mm["max"]
    if lo < lower or hi > upper:
        raise SyncError(f"{ctx}: data dates {lo}..{hi} outside declared interval {lower}..{upper}")
    return lo, hi


def validate_and_load_stage(
    cur: Any, source_dir: Path, entries: list[dict[str, Any]]
) -> dict[str, Any]:
    import pyarrow.parquet as pq

    active = [e for e in entries if e["rows"] > 0]
    if not active:
        raise SyncError("manifest has no non-empty partitions")
    for entry in active:
        path = source_dir / entry["file"]
        if not path.exists():
            raise SyncError(f"manifest file missing on disk: {entry['file']}")
    check_no_extra_files(source_dir, entries)

    manifest_rows = sum(e["rows"] for e in entries)
    for entry in active:
        path = source_dir / entry["file"]
        lower = date.fromisoformat(entry["start_date"])
        upper = date.fromisoformat(entry["end_date"])
        pf = pq.ParquetFile(path)
        missing = [c for c in REQUIRED_COLUMNS if c not in pf.schema_arrow.names]
        if missing:
            raise SyncError(f"{entry['file']}: missing columns {missing}")
        if pf.metadata.num_rows != entry["rows"]:
            raise SyncError(
                f"{entry['file']}: metadata rows {pf.metadata.num_rows} != manifest {entry['rows']}"
            )
        for bi, batch in enumerate(
            pf.iter_batches(batch_size=BATCH_ROWS, columns=list(REQUIRED_COLUMNS))
        ):
            if batch.num_rows == 0:
                raise SyncError(f"{entry['file']}[batch{bi}]: empty batch")
            ctx = f"{entry['file']}[batch{bi}]"
            batch_date_bounds(validate_batch(batch, ctx), ctx, lower, upper)
            copy_batch(cur, batch, ctx)

    cur.execute(
        "CREATE UNIQUE INDEX stage_stock_status_key ON stage_stock_status (stock_code, trade_date)"
    )
    cur.execute("ANALYZE stage_stock_status")
    cur.execute(
        """
        SELECT count(*), count(DISTINCT stock_code), count(DISTINCT trade_date),
               count(DISTINCT (stock_code, trade_date)),
               min(trade_date)::text, max(trade_date)::text,
               count(*) FILTER (WHERE is_st = 1),
               count(*) FILTER (WHERE is_st = 0),
               count(*) FILTER (WHERE is_suspend = 'Y'),
               count(*) FILTER (WHERE is_suspend = 'N')
        FROM stage_stock_status
        """
    )
    row = cur.fetchone()
    assert row is not None
    stats = {
        "manifest_rows": manifest_rows,
        "rows": row[0],
        "distinct_stock_codes": row[1],
        "distinct_trade_dates": row[2],
        "distinct_keys": row[3],
        "min_date": row[4],
        "max_date": row[5],
        "is_st_1": row[6],
        "is_st_0": row[7],
        "is_suspend_Y": row[8],
        "is_suspend_N": row[9],
    }
    if stats["rows"] != manifest_rows:
        raise SyncError(f"stage rows {stats['rows']} != manifest rows {manifest_rows}")
    if stats["rows"] != stats["distinct_keys"]:
        raise SyncError("duplicate keys detected in staged source data")
    return stats


def verify_source(source_dir: Path, manifest_name: str) -> dict[str, Any]:
    import pyarrow.parquet as pq

    if not source_dir.is_dir():
        raise SyncError(f"source directory does not exist: {source_dir}")
    entries = read_manifest(source_dir, manifest_name)
    check_manifest(entries)
    check_no_extra_files(source_dir, entries)
    totals: dict[str, Any] = {"files": 0, "rows": 0, "is_st_1": 0, "is_suspend_Y": 0}
    min_date: str | None = None
    max_date: str | None = None
    for entry in entries:
        if entry["rows"] == 0:
            continue
        path = source_dir / entry["file"]
        if not path.exists():
            raise SyncError(f"manifest file missing on disk: {entry['file']}")
        lower = date.fromisoformat(entry["start_date"])
        upper = date.fromisoformat(entry["end_date"])
        pf = pq.ParquetFile(path)
        missing = [c for c in REQUIRED_COLUMNS if c not in pf.schema_arrow.names]
        if missing:
            raise SyncError(f"{entry['file']}: missing columns {missing}")
        if pf.metadata.num_rows != entry["rows"]:
            raise SyncError(
                f"{entry['file']}: metadata rows {pf.metadata.num_rows} != manifest {entry['rows']}"
            )
        # Manifest ranges are disjoint, so a key duplicated across files would need the
        # same date in two ranges (impossible); per-file uniqueness is sufficient here.
        keys: set[tuple[str, date]] = set()
        for bi, batch in enumerate(
            pf.iter_batches(batch_size=BATCH_ROWS, columns=list(REQUIRED_COLUMNS))
        ):
            ctx = f"{entry['file']}[batch{bi}]"
            dates = validate_batch(batch, ctx)
            lo, hi = batch_date_bounds(dates, ctx, lower, upper)
            if min_date is None or lo.isoformat() < min_date:
                min_date = lo.isoformat()
            if max_date is None or hi.isoformat() > max_date:
                max_date = hi.isoformat()
            for code, dt in zip(batch.column("instrument").to_pylist(), dates.to_pylist()):
                key = (code, dt)
                if key in keys:
                    raise SyncError(f"{ctx}: duplicate key {code} {dt}")
                keys.add(key)
            totals["is_st_1"] += int(batch.column("is_risk_warning").cast("int64").sum().as_py())
            totals["is_suspend_Y"] += int(batch.column("suspended").cast("int64").sum().as_py())
        totals["files"] += 1
        totals["rows"] += entry["rows"]
    totals["min_date"] = min_date
    totals["max_date"] = max_date
    totals["declared_min_date"] = min(
        (e["start_date"] for e in entries if e["rows"] > 0), default=None
    )
    totals["declared_max_date"] = max(
        (e["end_date"] for e in entries if e["rows"] > 0), default=None
    )
    totals["is_st_0"] = totals["rows"] - totals["is_st_1"]
    totals["is_suspend_N"] = totals["rows"] - totals["is_suspend_Y"]
    return totals


# ---------------------------------------------------------------------------
# database helpers
# ---------------------------------------------------------------------------


def fetch_invariants(cur: Any, bounds: tuple[str, str] | None = None) -> dict[str, int]:
    cur.execute(INVARIANTS_SQL)
    row = cur.fetchone()
    assert row is not None
    invariants = {
        "target_rows": row[0],
        "matched": row[1],
        "matched_mismatch": row[2],
        "unmatched_nondefault": row[3],
        "is_st_bad": row[4],
        "suspend_bad": row[5],
        "src_absent": row[6],
        "tgt_unmatched": row[7],
    }
    if bounds is not None:
        lo, hi = bounds
        cur.execute(
            """
            SELECT
             (SELECT count(*) FROM stage_stock_status s
                WHERE s.trade_date BETWEEN %s AND %s
                  AND NOT EXISTS (SELECT 1 FROM stock_hist_unadj t
                                  WHERE t.stock_code = s.stock_code AND t.date = s.trade_date)),
             (SELECT count(*) FROM stock_hist_unadj t
                WHERE t.date BETWEEN %s AND %s
                  AND NOT EXISTS (SELECT 1 FROM stage_stock_status s
                                  WHERE s.stock_code = t.stock_code AND s.trade_date = t.date))
            """,
            (lo, hi, lo, hi),
        )
        win = cur.fetchone()
        assert win is not None
        invariants["src_absent_in_coverage"] = win[0]
        invariants["tgt_unmatched_in_coverage"] = win[1]
    return invariants


def fetch_distributions(cur: Any) -> dict[str, int]:
    cur.execute(DISTRIBUTIONS_SQL)
    row = cur.fetchone()
    assert row is not None
    return {
        "is_st_null": row[0],
        "is_st_0": row[1],
        "is_st_1": row[2],
        "is_suspend_N": row[3],
        "is_suspend_Y": row[4],
    }


def fetch_identity(cur: Any) -> dict[str, Any]:
    cur.execute(
        """
        SELECT current_database(), current_schema(), current_user,
               coalesce(inet_server_addr()::text, 'unix-socket'),
               coalesce(inet_server_port(), 0),
               substring(version() from 'PostgreSQL [0-9.]+'),
               pg_size_pretty(pg_database_size(current_database()))
        """
    )
    row = cur.fetchone()
    assert row is not None
    return {
        "database": row[0],
        "schema": row[1],
        "user": row[2],
        "server_addr": row[3],
        "server_port": row[4],
        "server_version": row[5],
        "database_size": row[6],
    }


def stage_temp_table(cur: Any, source_dir: Path, entries: list[dict[str, Any]]) -> dict[str, Any]:
    cur.execute("SET work_mem = '512MB'")
    cur.execute(STAGE_DDL)
    started = time.time()
    stats = validate_and_load_stage(cur, source_dir, entries)
    stats["seconds"] = round(time.time() - started, 1)
    return stats


# ---------------------------------------------------------------------------
# subcommands
# ---------------------------------------------------------------------------


def cmd_verify(args: argparse.Namespace) -> dict[str, Any]:
    source_dir = Path(args.source_dir)
    summary = verify_source(source_dir, args.manifest)
    return {"status": "success", "source": summary}


def cmd_preflight(args: argparse.Namespace) -> dict[str, Any]:
    import shutil

    source_dir = Path(args.source_dir)
    entries = read_manifest(source_dir, args.manifest)
    check_manifest(entries)
    result: dict[str, Any] = {}
    conn = connect("stock_status_preflight")
    try:
        with conn.cursor() as cur:
            result["source"] = stage_temp_table(cur, source_dir, entries)
            conn.commit()
            result["database"] = fetch_identity(cur)
            cur.execute(
                """
                SELECT count(*), min(date)::text, max(date)::text,
                       pg_size_pretty(pg_total_relation_size('stock_hist_unadj')),
                       pg_size_pretty(pg_indexes_size('stock_hist_unadj'))
                FROM stock_hist_unadj
                """
            )
            row = cur.fetchone()
            assert row is not None
            result["target"] = {
                "relation": "stock_hist_unadj",
                "rows": row[0],
                "min_date": row[1],
                "max_date": row[2],
                "total_size": row[3],
                "index_size": row[4],
            }
            bounds = (result["source"]["min_date"], result["source"]["max_date"])
            result["join"] = fetch_invariants(cur, bounds)
            result["coverage_source_bounds"] = {"start": bounds[0], "end": bounds[1]}
            cur.execute(
                """
                SELECT pg_size_pretty(pg_total_relation_size('stage_stock_status'))
                """
            )
            size_row = cur.fetchone()
            result["stage_size"] = size_row[0] if size_row else None
    finally:
        conn.close()
    result["filesystem"] = {
        "scope": "container filesystem (not necessarily the database host)",
        "free": f"{shutil.disk_usage('/').free / (1024**3):.2f} GiB",
    }
    result["status"] = "success"
    return result


def assert_invariants(cur: Any, baseline: dict[str, int], phase: str) -> dict[str, int]:
    current = fetch_invariants(cur)
    expected = {
        "target_rows": baseline["target_rows"],
        "matched": baseline["matched"],
        "matched_mismatch": 0,
        "unmatched_nondefault": 0,
        "is_st_bad": 0,
        "suspend_bad": 0,
        "src_absent": baseline["src_absent"],
        "tgt_unmatched": baseline["tgt_unmatched"],
    }
    bad = {key: (current[key], value) for key, value in expected.items() if current[key] != value}
    if bad:
        raise SyncError(f"{phase}: invariant violations {bad}")
    return current


def sample_rows(cur: Any, limit: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT t.stock_code, t.date::text, s.is_st, t.is_st, s.is_suspend, t.is_suspend
        FROM stock_hist_unadj t
        JOIN stage_stock_status s
          ON t.stock_code = s.stock_code AND t.date = s.trade_date
        ORDER BY t.stock_code, t.date
        LIMIT %s
        """,
        (limit,),
    )
    matched = [
        {
            "stock_code": code,
            "date": dt,
            "src_is_st": s_st,
            "is_st": t_st,
            "src_is_suspend": (s_sus or "").strip(),
            "is_suspend": (t_sus or "").strip(),
        }
        for code, dt, s_st, t_st, s_sus, t_sus in cur.fetchall()
    ]
    for row in matched:
        row["ok"] = row["src_is_st"] == row["is_st"] and row["src_is_suspend"] == row["is_suspend"]
    cur.execute(
        """
        SELECT t.stock_code, t.date::text, t.is_st, t.is_suspend
        FROM stock_hist_unadj t
        WHERE NOT EXISTS (
            SELECT 1 FROM stage_stock_status s
            WHERE s.stock_code = t.stock_code AND s.trade_date = t.date
        )
        ORDER BY t.stock_code, t.date
        LIMIT %s
        """,
        (max(1, limit // 2),),
    )
    unmatched = [
        {
            "stock_code": code,
            "date": dt,
            "is_st": t_st,
            "is_suspend": (t_sus or "").strip(),
            "ok": t_st is None and (t_sus or "").strip() == "N",
        }
        for code, dt, t_st, t_sus in cur.fetchall()
    ]
    return {
        "matched": matched,
        "matched_all_ok": all(r["ok"] for r in matched),
        "unmatched": unmatched,
        "unmatched_all_ok": all(r["ok"] for r in unmatched),
    }


def cmd_apply(args: argparse.Namespace) -> dict[str, Any]:
    if args.confirm != "FULL_OVERWRITE":
        raise SyncError("apply requires --confirm FULL_OVERWRITE")
    source_dir = Path(args.source_dir)
    entries = read_manifest(source_dir, args.manifest)
    check_manifest(entries)

    result: dict[str, Any] = {"committed": False, "errors": []}
    baseline: dict[str, int] = {}
    deterministic: list[tuple[str, str, int | None, str | None, int, str]] = []
    conn = connect("stock_status_apply")
    stage_stats: dict[str, Any] | None = None
    try:
        with conn.cursor() as cur:
            stage_stats = stage_temp_table(cur, source_dir, entries)
            conn.commit()
        result["source"] = stage_stats

        baseline: dict[str, int] = {}
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '10s'")
            cur.execute("SET LOCAL statement_timeout = '45min'")
            cur.execute("SET LOCAL idle_in_transaction_session_timeout = '10min'")
            locked = time.time()
            cur.execute("LOCK TABLE stock_hist_unadj IN SHARE ROW EXCLUSIVE MODE")
            result["lock_wait_seconds"] = round(time.time() - locked, 2)
            cur.execute("SHOW synchronous_commit")
            result["synchronous_commit"] = cur.fetchone()[0]

            cur.execute("SELECT count(*) FROM stock_hist_unadj")
            baseline["target_rows"] = cur.fetchone()[0]
            pre = fetch_invariants(cur, (stage_stats["min_date"], stage_stats["max_date"]))
            baseline["matched"] = pre["matched"]
            baseline["src_absent"] = pre["src_absent"]
            baseline["tgt_unmatched"] = pre["tgt_unmatched"]
            result["baseline"] = baseline
            result["pre_write_invariants"] = pre

            started = time.time()
            cur.execute(UPDATE_MATCHED_SQL)
            changed = cur.rowcount
            matched_seconds = time.time() - started
            started = time.time()
            cur.execute(UPDATE_UNMATCHED_SQL)
            reset = cur.rowcount
            unmatched_seconds = time.time() - started
            result["changed_matched_rows"] = changed
            result["reset_unmatched_rows"] = reset
            result["update_matched_seconds"] = round(matched_seconds, 1)
            result["update_unmatched_seconds"] = round(unmatched_seconds, 1)

            result["pre_commit_invariants"] = assert_invariants(cur, baseline, "pre-commit")
            result["pre_commit_distributions"] = fetch_distributions(cur)

            started = time.time()
            conn.commit()
            result["commit_seconds"] = round(time.time() - started, 2)
            result["committed"] = True

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '10min'")
            result["post_commit_invariants"] = assert_invariants(cur, baseline, "post-commit")
            result["post_commit_distributions"] = fetch_distributions(cur)
            samples = sample_rows(cur, args.sample_size)
            result["samples"] = samples
            result["post_commit_checks_ok"] = (
                samples["matched_all_ok"] and samples["unmatched_all_ok"]
            )
            deterministic = [
                (
                    r["stock_code"],
                    r["date"],
                    r["is_st"],
                    r["is_suspend"],
                    r["src_is_st"],
                    r["src_is_suspend"],
                )
                for r in samples["matched"]
            ]
    except SyncError as exc:
        result["errors"].append(f"aborted: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
    except Exception as exc:
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if result["committed"] and not result["errors"]:
        try:
            conn2 = connect("stock_status_verify")
            with conn2.cursor() as cur:
                result["fresh_identity"] = fetch_identity(cur)
                cur.execute("SELECT count(*) FROM stock_hist_unadj")
                row_count = cur.fetchone()[0]
                dists = fetch_distributions(cur)
                cur.execute(
                    """
                    SELECT count(*) FILTER (WHERE is_st IS NOT NULL AND is_st NOT IN (0, 1)),
                           count(*) FILTER (WHERE is_suspend IS NULL
                                              OR is_suspend NOT IN ('N', 'Y'))
                    FROM stock_hist_unadj
                    """
                )
                bad = cur.fetchone()
                checks_ok = (
                    row_count == baseline["target_rows"]
                    and bad[0] == 0
                    and bad[1] == 0
                    and dists == result["post_commit_distributions"]
                )
                for code, dt, exp_st, exp_sus, _src_st, _src_sus in deterministic:
                    cur.execute(
                        "SELECT is_st, is_suspend FROM stock_hist_unadj "
                        "WHERE stock_code = %s AND date = %s",
                        (code, dt),
                    )
                    sampled = cur.fetchone()
                    checks_ok = (
                        checks_ok
                        and sampled is not None
                        and sampled[0] == exp_st
                        and ((sampled[1] or "").strip() == (exp_sus or "").strip())
                    )
                result["fresh_connection_check"] = {
                    "target_rows": row_count,
                    "distributions": dists,
                    "is_st_bad": bad[0],
                    "suspend_bad": bad[1],
                    "deterministic_samples_checked": len(deterministic),
                    "ok": checks_ok,
                }
                if not checks_ok:
                    result["errors"].append("fresh connection verification mismatch")
            conn2.close()
        except Exception as exc:
            result["errors"].append(f"fresh connection check failed: {type(exc).__name__}: {exc}")
    elif result["committed"]:
        result["commit_ambiguous"] = True
        result["errors"].append(
            "commit succeeded but verification reported errors; do not rerun blindly"
        )

    if result["committed"] and not result["errors"]:
        result["status"] = "success"
    elif result["committed"]:
        result["status"] = "committed_with_concerns"
    else:
        result["status"] = "not_committed"
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sync_stock_status.py",
        description=(
            "Validate BigQuant cn_stock_status exports and sync "
            "stock_hist_unadj.is_st / is_suspend."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(target: argparse.ArgumentParser) -> None:
        target.add_argument(
            "--source-dir", required=True, help="Directory with *.parquet + manifest"
        )
        target.add_argument("--manifest", default=MANIFEST_NAME, help="Manifest file name")

    verify = sub.add_parser("verify", help="Source-only validation, no database access")
    add_common(verify)
    verify.set_defaults(func=cmd_verify)

    preflight = sub.add_parser("preflight", help="Stage into TEMP table and report coverage")
    add_common(preflight)
    preflight.set_defaults(func=cmd_preflight)

    apply_cmd = sub.add_parser("apply", help="Atomically overwrite is_st / is_suspend")
    add_common(apply_cmd)
    apply_cmd.add_argument(
        "--confirm",
        required=True,
        help="Must be the literal FULL_OVERWRITE",
    )
    apply_cmd.add_argument("--sample-size", type=int, default=20, help="Matched samples to verify")
    apply_cmd.set_defaults(func=cmd_apply)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = args.func(args)
    except SyncError as exc:
        emit({"status": "failed", "errors": [str(exc)]})
        return 2
    except Exception as exc:
        emit({"status": "failed", "errors": [f"{type(exc).__name__}: {exc}"]})
        return 2
    emit(result)
    return 0 if result.get("status") == "success" else 2


if __name__ == "__main__":
    raise SystemExit(main())
