from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,name,market"
_DEFAULT_MARKETS = ("CSI", "SSE", "SZSE")


@dataclass(frozen=True)
class TushareIndexBasicFetcher(Fetcher):
    """Fetch index_basic data and merge with CSV overrides."""

    client: TushareClient
    retry_policy: RetryPolicy
    page_size: int = 4000

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw index_basic rows for target markets and CSV overrides."""
        params = chunk_args.get("params") or {}
        fields = str(params.get("fields", _DEFAULT_FIELDS))
        markets = _normalize_markets(params.get("markets", _DEFAULT_MARKETS))
        csv_path = str(params.get("csv_path", "extra/additional_index_info.csv"))

        tushare_rows: list[dict[str, object]] = []
        for market in markets:
            offset = 0
            pages = 0
            while True:
                batch = self.retry_policy.execute(
                    lambda: _safe_index_basic(
                        self.client,
                        market,
                        fields,
                        offset,
                        self.page_size,
                    )
                )
                if not batch:
                    logger.info(
                        "tushare index_basic exhausted",
                        extra={
                            "market": market,
                            "pages": pages,
                            "total_rows": len(tushare_rows),
                        },
                    )
                    break
                tushare_rows.extend(batch)
                pages += 1
                logger.debug(
                    "tushare index_basic page",
                    extra={
                        "market": market,
                        "offset": offset,
                        "limit": self.page_size,
                        "batch_size": len(batch),
                    },
                )
                offset += self.page_size

        csv_rows = _load_csv(csv_path)
        logger.info(
            "index_basic csv loaded",
            extra={"csv_path": csv_path, "row_count": len(csv_rows)},
        )
        return [{"tushare": tushare_rows, "csv": csv_rows}]


def _safe_index_basic(
    client: TushareClient,
    market: str,
    fields: str,
    offset: int,
    limit: int,
) -> list[dict[str, object]]:
    try:
        return client.index_basic(market=market, fields=fields, offset=offset, limit=limit)
    except Exception as exc:
        raise RetryableError(str(exc)) from exc


def _normalize_markets(value: object) -> list[str]:
    if value is None:
        return list(_DEFAULT_MARKETS)
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    raise ValueError("markets must be list[str] or comma-separated string")


def _load_csv(path: str) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    rows: list[dict[str, str]] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if len(row) != 3:
                raise ValueError("additional_index_info.csv must have 3 columns per row")
            index_code, index_name, market = (item.strip() for item in row)
            rows.append(
                {
                    "ts_code": index_code,
                    "name": index_name,
                    "market": market,
                }
            )
    return rows
