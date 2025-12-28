from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from time import sleep

from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.registry import PipelineRegistry
from core.pipeline.types import ChunkArgs, NormalizedBatch, RawBatch
from core.pipeline.validation import ensure_hashable
from core.workflow.failover import FetchCleanFailoverPolicy, PipelineFailoverPolicy, Stage
from infra.db.repository import Repository
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState
from services.pipeline_selector import PipelineSelector

logger = logging.getLogger(__name__)


class StageError(RuntimeError):
    """Error wrapper containing workflow stage context."""

    def __init__(self, stage: Stage, error: Exception, chunk_args: ChunkArgs | None = None) -> None:
        super().__init__(str(error))
        self.stage = stage
        self.original = error
        self.chunk_args = chunk_args


@dataclass
class WorkflowEngine:
    """Pipeline-aware workflow orchestrator."""

    store: TaskStatusStore
    registry: PipelineRegistry
    selector: PipelineSelector
    repo: Repository
    failover: PipelineFailoverPolicy = FetchCleanFailoverPolicy()

    def handle(self, task_id: int, task: PipelineTask) -> None:
        """Run a task through pipeline execution and persistence."""
        _ = self.store.get_by_id(task_id)
        task_payload = self.store.get_task_payload(task_id)
        arguments_digest = ensure_hashable(task_payload.arguments)
        run_started_at = time.monotonic()

        if task_payload.spec == TaskSpec.NOOP_SLEEP:
            self.store.update_state(task_id, TaskState.RUNNING)
            sleep(5)
            self.store.update_progress(task_id, Decimal("100"))
            self.store.update_state(task_id, TaskState.SUCCEEDED)
            return

        candidates = self.selector.candidates_for(task_payload.spec)
        if not candidates:
            raise ValueError(f"no pipeline candidates for {task_payload.spec}")

        self.store.update_state(task_id, TaskState.RUNNING)
        self.store.update_progress(task_id, Decimal("0"))
        logger.info(
            "run started",
            extra={
                "task_id": task_id,
                "pipeline_id": task_payload.pipeline_id,
                "arguments_digest": arguments_digest,
            },
        )
        logger.info(
            "pipeline candidates selected",
            extra={"task_id": task_id, "candidates": candidates},
        )

        current_pipeline_id = candidates[0]
        consecutive_failures = 0
        while current_pipeline_id is not None:
            try:
                pipeline = self.registry.get(current_pipeline_id)
                total_chunks, total_persisted = self._run_pipeline(task_id, task_payload, pipeline)
                self.store.update_state(task_id, TaskState.SUCCEEDED)
                total_duration_ms = int((time.monotonic() - run_started_at) * 1000)
                logger.info(
                    "run succeeded",
                    extra={
                        "task_id": task_id,
                        "total_duration_ms": total_duration_ms,
                        "total_chunks": total_chunks,
                        "final_stats": {"persisted": total_persisted},
                    },
                )
                return
            except Exception as exc:
                if isinstance(exc, StageError):
                    stage = exc.stage
                    error = exc.original
                    chunk_args = exc.chunk_args
                else:
                    stage = Stage.PERSIST
                    error = exc
                    chunk_args = None

                if not self.failover.should_failover(error, stage, chunk_args):
                    logger.error(
                        "run failed",
                        extra={
                            "task_id": task_id,
                            "error_type": type(error).__name__,
                            "error_summary": str(error),
                            "failed_stage": stage.value,
                        },
                    )
                    self.store.update_state(task_id, TaskState.FAILED, error=str(error))
                    return
                consecutive_failures += 1
                logger.warning(
                    "failover triggered",
                    extra={
                        "task_id": task_id,
                        "reason_code": type(error).__name__,
                        "failed_stage": stage.value,
                        "consecutive_failures": consecutive_failures,
                    },
                )
                next_id = self.failover.select_next_pipeline(current_pipeline_id, candidates, error)
                if next_id is None:
                    logger.error(
                        "failover unavailable",
                        extra={
                            "task_id": task_id,
                            "from_pipeline": current_pipeline_id,
                        },
                    )
                    self.store.update_state(task_id, TaskState.FAILED, error=str(error))
                    return
                logger.warning(
                    "pipeline failover",
                    extra={
                        "task_id": task_id,
                        "from_pipeline": current_pipeline_id,
                        "to_pipeline": next_id,
                    },
                )
                current_pipeline_id = next_id
                self.store.update_progress(task_id, Decimal("0"))

    def _run_pipeline(
        self, task_id: int, task: PipelineTask, pipeline: IngestionPipeline
    ) -> tuple[int, int]:
        chunks = pipeline.plan_chunks(task.arguments)
        total = len(chunks)
        strategy_label = getattr(pipeline, "chunk_strategy", type(pipeline).__name__)
        logger.info(
            "chunk planning completed",
            extra={
                "task_id": task_id,
                "total_chunks": total,
                "chunk_strategy": strategy_label,
            },
        )
        if total > 0:
            sample = [_safe_digest(chunk) for chunk in chunks[:3]]
            logger.debug(
                "chunk samples",
                extra={"task_id": task_id, "chunk_args_digest_sample": sample},
            )
        if total == 0:
            self.store.update_progress(task_id, Decimal("100"))
            return 0, 0
        done = 0
        total_persisted = 0
        for index, chunk_args in enumerate(chunks, start=1):
            current_chunk = chunk_args
            chunk_digest = _safe_digest(current_chunk)
            chunk_started_at = time.monotonic()
            logger.info(
                "chunk started",
                extra={"task_id": task_id, "chunk_id": index, "chunk_args_digest": chunk_digest},
            )
            try:
                raw_batch = self._fetch(pipeline, current_chunk)
                normalized = self._clean(pipeline, raw_batch, current_chunk)
                persisted, normalized_count = self._persist(normalized)
            except Exception as exc:
                failed_stage = (
                    exc.stage.value if isinstance(exc, StageError) else Stage.PERSIST.value
                )
                attempts_used = getattr(
                    exc.original if isinstance(exc, StageError) else exc, "attempts_used", None
                )
                logger.error(
                    "chunk failed",
                    extra={
                        "task_id": task_id,
                        "chunk_id": index,
                        "chunk_args_digest": chunk_digest,
                        "failed_stage": failed_stage,
                        "attempts_used": attempts_used,
                        "error_type": type(exc).__name__,
                    },
                )
                raise exc
            done += 1
            total_persisted += persisted
            progress = (Decimal(done) / Decimal(total)) * Decimal("100")
            self.store.update_progress(task_id, progress)
            duration_ms = int((time.monotonic() - chunk_started_at) * 1000)
            logger.info(
                "chunk succeeded",
                extra={
                    "task_id": task_id,
                    "chunk_id": index,
                    "duration_ms": duration_ms,
                    "stats": {
                        "normalized_count": normalized_count,
                        "persisted": persisted,
                    },
                },
            )
        return total, total_persisted

    def _fetch(self, pipeline: IngestionPipeline, chunk_args: ChunkArgs) -> RawBatch:
        try:
            return pipeline.fetch(chunk_args)
        except Exception as exc:
            raise StageError(Stage.FETCH, exc, chunk_args) from exc

    def _clean(
        self, pipeline: IngestionPipeline, raw_batch: RawBatch, chunk_args: ChunkArgs
    ) -> NormalizedBatch:
        try:
            return pipeline.clean(raw_batch)
        except Exception as exc:
            raise StageError(Stage.CLEAN, exc, chunk_args) from exc

    def _persist(self, normalized: NormalizedBatch) -> tuple[int, int]:
        records = list(normalized)
        if not records:
            return 0, 0
        persisted = self.repo.insert_batch(records)
        return persisted, len(records)


def _safe_digest(payload: ChunkArgs) -> str:
    try:
        return ensure_hashable(payload)
    except Exception:
        return "unhashable"
