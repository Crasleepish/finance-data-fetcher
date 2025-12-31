from __future__ import annotations

import logging
from dataclasses import dataclass

from core.fetch.errors import RetryableError
from core.fetch.fetcher import Fetcher
from core.fetch.retry import RetryPolicy
from core.pipeline.types import ChunkArgs, RawBatch
from infra.tushare.client import TushareClient

logger = logging.getLogger(__name__)

_DEFAULT_FIELDS = "ts_code,name,fund_type,invest_type,found_date,m_fee,c_fee,market"


@dataclass(frozen=True)
class TushareFundBasicFetcher(Fetcher):
    """Fetch fund_basic data from Tushare with internal pagination."""

    client: TushareClient
    retry_policy: RetryPolicy
    page_size: int = 10000

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_basic data for the given chunk arguments."""
        params = chunk_args.get("params") or {}
        market = str(params.get("market", "O"))
        status = str(params.get("status", "L"))
        fields = str(params.get("fields", _DEFAULT_FIELDS))

        all_rows: list[dict[str, object]] = []
        offset = 0
        pages = 0
        while True:
            batch = self.retry_policy.execute(
                lambda: _safe_fund_basic(
                    self.client,
                    market,
                    status,
                    fields,
                    offset,
                    self.page_size,
                )
            )
            if not batch:
                logger.info(
                    "tushare fund_basic exhausted",
                    extra={
                        "market": market,
                        "status": status,
                        "pages": pages,
                        "total_rows": len(all_rows),
                    },
                )
                break
            all_rows.extend(batch)
            pages += 1
            logger.debug(
                "tushare fund_basic page",
                extra={
                    "market": market,
                    "status": status,
                    "offset": offset,
                    "limit": self.page_size,
                    "batch_size": len(batch),
                },
            )
            offset += self.page_size
        return all_rows


def _safe_fund_basic(
    client: TushareClient,
    market: str,
    status: str,
    fields: str,
    offset: int,
    limit: int,
) -> list[dict[str, object]]:
    try:
        return client.fund_basic(
            market=market,
            status=status,
            fields=fields,
            offset=offset,
            limit=limit,
        )
    except Exception as exc:
        raise RetryableError(str(exc)) from exc
