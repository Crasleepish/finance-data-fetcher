from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from core.pipeline.types import ChunkArgs


class Stage(StrEnum):
    """Execution stages for workflow failover decisions."""

    PLAN = "plan"
    FETCH = "fetch"
    CLEAN = "clean"
    PERSIST = "persist"


class PipelineFailoverPolicy(Protocol):
    """Policy interface for pipeline failover decisions."""

    def should_failover(self, error: Exception, stage: Stage, chunk_args: ChunkArgs | None) -> bool:
        """Return True if workflow should switch pipelines."""

    def select_next_pipeline(
        self, current: str, candidates: list[str], error: Exception
    ) -> str | None:
        """Select the next pipeline id or None to stop."""


@dataclass(frozen=True)
class FetchCleanFailoverPolicy:
    """Failover on fetch/clean errors, restart pipeline from the beginning."""

    def should_failover(self, error: Exception, stage: Stage, chunk_args: ChunkArgs | None) -> bool:
        return stage in {Stage.FETCH, Stage.CLEAN}

    def select_next_pipeline(
        self, current: str, candidates: list[str], error: Exception
    ) -> str | None:
        if current not in candidates:
            return None
        index = candidates.index(current)
        if index + 1 >= len(candidates):
            return None
        return candidates[index + 1]
