#!/usr/bin/env python3
"""Export BigQuant ``cn_stock_status`` to quarterly Parquet partitions.

Runs inside BigQuant AIStudio (``dai`` is imported only at query time, so
``--help`` works on a plain Python install).
"""

from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path

DEFAULT_OUT_DIR = "/home/aiuser/work/cn_stock_status_export"
MANIFEST_NAME = "manifest.csv"

SQL_TEMPLATE = """
SELECT
    date,
    instrument,
    is_risk_warning,
    suspended
FROM cn_stock_status
WHERE date >= '{start}'
  AND date <= '{end}'
ORDER BY date, instrument
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export BigQuant cn_stock_status to quarterly Parquet files.",
    )
    parser.add_argument("--start", required=True, help="Inclusive start date, e.g. 2005-01-01")
    parser.add_argument("--end", required=True, help="Inclusive end date, e.g. 2025-12-31")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Local output directory")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Arrow batch size")
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip already complete partitions (use --no-skip-existing to force refresh)",
    )
    return parser.parse_args(argv)


def write_manifest(out_dir: Path, manifest: list[dict]) -> None:
    import pandas as pd

    pd.DataFrame(manifest).to_csv(out_dir / MANIFEST_NAME, index=False)


def validate_existing(path: Path) -> int:
    """Return row count of an existing partition file, validating its metadata."""
    import pyarrow.parquet as pq

    metadata = pq.read_metadata(path)
    if metadata.num_rows <= 0:
        raise ValueError(f"existing file has no rows: {path}")
    return metadata.num_rows


def export_partition(
    out_dir: Path,
    start_str: str,
    end_str: str,
    filename: str,
    batch_size: int,
) -> int:
    """Query one period and stream it to Parquet; returns the row count."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    temp_path = out_dir / f"{filename}.part"
    if temp_path.exists():
        temp_path.unlink()

    sql = SQL_TEMPLATE.format(start=start_str, end=end_str)
    result = dai.query(sql, filters={"date": [start_str, end_str]})
    reader = result.fetch_arrow_reader(batch_size=batch_size)

    writer = None
    row_count = 0
    batch_count = 0
    try:
        for batch in reader:
            batch_count += 1
            row_count += batch.num_rows
            table = pa.Table.from_batches([batch])
            if writer is None:
                writer = pq.ParquetWriter(
                    temp_path,
                    table.schema,
                    compression="zstd",
                    use_dictionary=True,
                )
            writer.write_table(table)
            print(
                f"  batch={batch_count:>3} | "
                f"batch_rows={batch.num_rows:>8,} | "
                f"total_rows={row_count:>10,}",
                flush=True,
            )
    finally:
        if writer is not None:
            writer.close()

    if row_count == 0:
        if temp_path.exists():
            temp_path.unlink()
        return 0

    temp_path.rename(out_dir / filename)
    return row_count


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    import pandas as pd

    global dai
    try:
        import dai  # noqa: F401  (imported only when executing the export)
    except ImportError:
        print("ERROR: this script must run inside BigQuant AIStudio (module 'dai' missing)")
        return 2

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    overall_start = pd.Timestamp(args.start).normalize()
    overall_end = pd.Timestamp(args.end).normalize()
    if overall_start > overall_end:
        print("ERROR: --start must not be later than --end", file=sys.stderr)
        return 2

    periods = pd.period_range(start=overall_start, end=overall_end, freq="Q")
    manifest: list[dict] = []

    for period in periods:
        chunk_start = max(overall_start, period.start_time.normalize())
        chunk_end = min(overall_end, period.end_time.normalize())
        start_str = chunk_start.strftime("%Y-%m-%d")
        end_str = chunk_end.strftime("%Y-%m-%d")
        filename = (
            f"cn_stock_status_{chunk_start.strftime('%Y%m%d')}"
            f"_{chunk_end.strftime('%Y%m%d')}.parquet"
        )
        filepath = out_dir / filename

        print()
        print("=" * 70)
        print(f"Processing: {start_str} -> {end_str}")
        print(f"Target:     {filepath}")
        print("=" * 70, flush=True)

        if args.skip_existing and filepath.exists():
            row_count = validate_existing(filepath)
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(
                f"Already exists, skipped | rows={row_count:,} | size={size_mb:.2f} MB", flush=True
            )
            manifest.append(
                {
                    "file": filename,
                    "start_date": start_str,
                    "end_date": end_str,
                    "rows": row_count,
                    "size_mb": round(size_mb, 3),
                    "status": "existing",
                }
            )
            write_manifest(out_dir, manifest)
            continue

        row_count = export_partition(out_dir, start_str, end_str, filename, args.batch_size)

        if row_count == 0:
            print("WARNING: 此时间段返回 0 行", flush=True)
            manifest.append(
                {
                    "file": filename,
                    "start_date": start_str,
                    "end_date": end_str,
                    "rows": 0,
                    "size_mb": 0,
                    "status": "empty",
                }
            )
        else:
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"DONE | rows={row_count:,} | size={size_mb:.2f} MB", flush=True)
            manifest.append(
                {
                    "file": filename,
                    "start_date": start_str,
                    "end_date": end_str,
                    "rows": row_count,
                    "size_mb": round(size_mb, 3),
                    "status": "downloaded",
                }
            )

        write_manifest(out_dir, manifest)
        gc.collect()

    write_manifest(out_dir, manifest)

    manifest_df = pd.DataFrame(manifest)
    print()
    print("=" * 70)
    print("EXPORT_FINISHED")
    print("=" * 70)
    print(manifest_df.to_string(index=False))
    print()
    print(f"Total rows: {int(manifest_df['rows'].sum()):,}")
    print(f"Total size: {manifest_df['size_mb'].sum():,.2f} MB")
    print()
    print(f"Output directory: {out_dir.resolve()}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # surface a stable failure marker for log polling
        print(f"EXPORT_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
