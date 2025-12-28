from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from time import sleep

from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.registry import PipelineRegistry
from core.pipeline.types import ChunkArgs, NormalizedBatch, RawBatch
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

        current_pipeline_id = candidates[0]
        while current_pipeline_id is not None:
            try:
                pipeline = self.registry.get(current_pipeline_id)
                self._run_pipeline(task_id, task_payload, pipeline)
                self.store.update_state(task_id, TaskState.SUCCEEDED)
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
                    self.store.update_state(task_id, TaskState.FAILED, error=str(error))
                    return
                next_id = self.failover.select_next_pipeline(current_pipeline_id, candidates, error)
                if next_id is None:
                    self.store.update_state(task_id, TaskState.FAILED, error=str(error))
                    return
                logger.warning(
                    "pipeline failover",
                    extra={"task_id": task_id, "from": current_pipeline_id, "to": next_id},
                )
                current_pipeline_id = next_id
                self.store.update_progress(task_id, Decimal("0"))

    def _run_pipeline(self, task_id: int, task: PipelineTask, pipeline: IngestionPipeline) -> None:
        chunks = pipeline.plan_chunks(task.arguments)
        total = len(chunks)
        if total == 0:
            self.store.update_progress(task_id, Decimal("100"))
            return
        done = 0
        for chunk_args in chunks:
            current_chunk = chunk_args
            try:
                raw_batch = self._fetch(pipeline, current_chunk)
                normalized = self._clean(pipeline, raw_batch, current_chunk)
                self._persist(normalized)
            except Exception as exc:
                logger.exception(
                    "chunk failed",
                    extra={"task_id": task_id, "chunk_args": current_chunk},
                )
                raise exc
            done += 1
            progress = (Decimal(done) / Decimal(total)) * Decimal("100")
            self.store.update_progress(task_id, progress)

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

    def _persist(self, normalized: NormalizedBatch) -> None:
        records = list(normalized)
        if not records:
            return
        self.repo.insert_batch(records)
