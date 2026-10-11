from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import Table, distinct, select
from sqlalchemy.engine import Engine

from core.clean.stock_code_change_cleaner import StockCodeChangeCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch

if TYPE_CHECKING:
    from infra.fetcher.ftshare_code_change_fetcher import FtshareCodeChangeFetcher

logger = logging.getLogger(__name__)

_SUFFIXED_CODE_RE = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")

# Explicit user-approved legacy-code exclusion (live sync task 128).
# These three malformed identifiers are skipped verbatim to unblock the run and
# represent a recorded coverage gap; no implicit normalization/mapping is applied
# and any OTHER malformed code still aborts the universe.
_EXCLUDED_UNIVERSE_CODES = frozenset({"T600018.SH", "T00018.SH", "TS0018.SH"})


@dataclass(frozen=True)
class StockCodeChangePipeline(IngestionPipeline):
    """Fetch and validate the full-market stock code-change snapshot.

    A single chunk gathers the whole candidate universe, fetches every code
    change from FTShare, validates the snapshot globally, and persists it via
    the workflow's atomic replace path.
    """

    basic_fetcher: Any
    code_change_fetcher: FtshareCodeChangeFetcher
    engine: Engine
    stock_hist_table: Table
    stock_info_table: Table
    _cleaner: StockCodeChangeCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_cleaner", StockCodeChangeCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Return one chunk covering the complete universe."""
        codes = self._gather_universe()
        logger.info("stock code change universe gathered", extra={"universe_count": len(codes)})
        return [{"params": {"codes": codes}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw code-change rows for the whole universe."""
        raw_chunk = dict(chunk_args)
        params = cast(dict[str, Any], raw_chunk["params"])
        codes = list(params["codes"])
        cancel_check = cast(Any, raw_chunk.get("cancel_check"))
        rows = self.code_change_fetcher.fetch(codes, cancel_check=cancel_check)
        logger.info(
            "stock code change fetch completed",
            extra={
                "universe_count": len(codes),
                "ftshare_attempts": getattr(self.code_change_fetcher, "last_request_count", None),
            },
        )
        return rows

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize and validate raw code-change rows."""
        return self._cleaner.clean(raw_batch)

    def _gather_universe(self) -> list[str]:
        """Collect candidate codes from TuShare stock_basic and local tables.

        Any malformed source record/value or missing fresh-listed set aborts the
        run before any FTShare call.
        """
        basic_rows = self.basic_fetcher.fetch(
            {"params": {"list_statuses": ["L", "D", "P"], "exchange": ""}}
        )
        codes: set[str] = set()
        listed_codes: set[str] = set()
        excluded_basic = 0
        for row in basic_rows:
            if not isinstance(row, Mapping):
                raise ValueError("malformed stock_basic record")
            code = _exclusion_aware_code(_extract_code(row))
            if code is None:
                excluded_basic += 1
                continue
            codes.add(code)
            if row.get("list_status") == "L":
                listed_codes.add(code)
        if not listed_codes:
            raise ValueError("stock code change universe has no fresh listed (L) codes")
        excluded_db = 0
        with self.engine.begin() as connection:
            history_codes = connection.execute(
                select(distinct(self.stock_hist_table.c.stock_code))
            ).scalars()
            for value in history_codes:
                code = _exclusion_aware_code(_require_code_value(value))
                if code is None:
                    excluded_db += 1
                    continue
                codes.add(code)
            info_codes = connection.execute(
                select(distinct(self.stock_info_table.c.stock_code))
            ).scalars()
            for value in info_codes:
                code = _exclusion_aware_code(_require_code_value(value))
                if code is None:
                    excluded_db += 1
                    continue
                codes.add(code)
        excluded_count = excluded_basic + excluded_db
        if excluded_count:
            logger.info(
                "stock code change universe excluded legacy codes",
                extra={
                    "excluded_count": excluded_count,
                    "excluded_basic_count": excluded_basic,
                    "excluded_db_count": excluded_db,
                },
            )
        if not codes:
            raise ValueError("stock code change universe is empty")
        return sorted(codes)


def _extract_code(row: Mapping[str, Any]) -> str:
    value = row.get("ts_code") or row.get("stock_code")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("malformed stock_basic code")
    return value.strip()


def _require_code_value(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"malformed universe code value: {value!r}")
    return value.strip()


def _exclusion_aware_code(value: str) -> str | None:
    """Return the validated code, or ``None`` for user-approved legacy exclusions."""
    if value in _EXCLUDED_UNIVERSE_CODES:
        return None
    return _require_suffixed_code(value)


def _require_suffixed_code(value: str) -> str:
    if not _SUFFIXED_CODE_RE.match(value):
        raise ValueError(f"invalid universe code, expected suffixed .SH/.SZ/.BJ: {value!r}")
    return value
