from __future__ import annotations

import csv
import zipfile
from dataclasses import dataclass
from pathlib import Path

from core.fetch.errors import NonRetryableError
from core.fetch.fetcher import Fetcher
from core.pipeline.types import ChunkArgs, RawBatch

_DATE_COLUMN = "日期"
_CODE_COLUMN = "代码"
_IS_ST_COLUMN = "是否ST"


@dataclass(frozen=True)
class StockIsStFromFileFetcher(Fetcher):
    """Fetch stock is_st rows from selected CSV members in a zip archive."""

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        params = chunk_args.get("params")
        if not isinstance(params, dict):
            raise NonRetryableError("params must be a mapping")

        zip_path = params.get("zip_path")
        member_names = params.get("member_names")
        if not isinstance(zip_path, str) or not zip_path:
            raise NonRetryableError("zip_path must be a string")
        if not isinstance(member_names, list) or not all(
            isinstance(name, str) for name in member_names
        ):
            raise NonRetryableError("member_names must be a list of strings")

        path = Path(zip_path)
        if not path.exists():
            raise NonRetryableError(f"zip file not found: {path}")

        rows: list[dict[str, str]] = []
        with zipfile.ZipFile(path) as archive:
            for member_name in member_names:
                rows.extend(_read_member_rows(archive, member_name))
        return rows


def _read_member_rows(archive: zipfile.ZipFile, member_name: str) -> list[dict[str, str]]:
    with archive.open(member_name) as handle:
        payload = handle.read()
    text = _decode_payload(payload)
    reader = csv.DictReader(text.splitlines())
    if reader.fieldnames is None:
        raise NonRetryableError(f"csv header missing: {member_name}")
    required = {_DATE_COLUMN, _CODE_COLUMN, _IS_ST_COLUMN}
    if not required.issubset(reader.fieldnames):
        raise NonRetryableError(f"csv required columns missing: {member_name}")
    return [
        {
            "date": row[_DATE_COLUMN],
            "code": row[_CODE_COLUMN],
            "is_st": row[_IS_ST_COLUMN],
        }
        for row in reader
    ]


def _decode_payload(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "gbk"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise NonRetryableError("zip member encoding is unsupported")
