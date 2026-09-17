from __future__ import annotations

from dataclasses import dataclass, field

from core.clean.etf_info_cleaner import EtfInfoCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fetcher.tushare_fund_basic_fetcher import TushareFundBasicFetcher
from infra.tushare.client import TushareClient

_ALLOWED_STATUSES = ("L", "D", "I")


@dataclass(frozen=True)
class EtfInfoPipeline(IngestionPipeline):
    """Pipeline for fetching and cleaning etf_info records."""

    client: TushareClient
    retry_policy: RetryPolicy
    _fetcher: TushareFundBasicFetcher = field(init=False, repr=False)
    _cleaner: EtfInfoCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            TushareFundBasicFetcher(
                client=self.client,
                retry_policy=self.retry_policy,
                page_size=4000,
            ),
        )
        object.__setattr__(self, "_cleaner", EtfInfoCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Return one chunk per run; the fetcher paginates every status inside it.

        Keeping all statuses in a single chunk means the workflow performs one
        fetch -> clean -> persist cycle, so L/D/I rows commit atomically.
        """
        params = dict(arguments.get("params", {}))
        market = params.pop("market", "E")
        statuses = _normalize_statuses(params.pop("status", params.pop("statuses", None)))
        return [{"params": {**params, "market": market, "statuses": statuses}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch raw fund_basic data for the chunk."""
        return self._fetcher.fetch(chunk_args)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Clean raw fund_basic data into etf_info records."""
        return self._cleaner.clean(raw_batch)


def _normalize_statuses(value: object) -> list[str]:
    """Normalize an optional status selector to a deduped, canonically ordered list."""
    if value is None:
        return list(_ALLOWED_STATUSES)
    if isinstance(value, str):
        raw = value.split(",")
    elif isinstance(value, (list, tuple)):
        raw = [str(item) for item in value]
    else:
        raise ValueError("status must be a string or a list of strings")
    statuses: list[str] = []
    for item in raw:
        status = item.strip()
        if status not in _ALLOWED_STATUSES:
            raise ValueError(
                f"invalid status: {status!r}; expected one of {', '.join(_ALLOWED_STATUSES)}"
            )
        if status not in statuses:
            statuses.append(status)
    if not statuses:
        raise ValueError("status must not be empty")
    statuses.sort(key=_ALLOWED_STATUSES.index)
    return statuses
