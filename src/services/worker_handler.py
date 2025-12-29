from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from core.pipeline.registry import PipelineRegistry
from core.pipeline.types import ChunkArgs, RawBatch
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineTaskHandler:
    """Execute pipeline tasks using a registry of pipelines."""

    registry: PipelineRegistry

    def handle(self, task_id: int, task: PipelineTask) -> None:
        """Run a task based on its spec."""
        if task.spec == TaskSpec.NOOP_SLEEP:
            time.sleep(5)
            return

        if task.spec != TaskSpec.PIPELINE:
            raise ValueError(f"Unknown task spec: {task.spec}")

        if task.pipeline_id is None:
            raise ValueError("pipeline_id is required for PIPELINE tasks")
        pipeline = self.registry.get(task.pipeline_id)
        chunks = pipeline.plan_chunks(task.arguments)
        for chunk in chunks:
            raw_batch = pipeline.fetch(_ensure_chunk_args(chunk))
            pipeline.clean(_ensure_raw_batch(raw_batch))

        logger.info("task pipeline completed", extra={"task_id": task_id})


def _ensure_chunk_args(chunk: ChunkArgs) -> ChunkArgs:
    return chunk


def _ensure_raw_batch(raw: RawBatch) -> RawBatch:
    return raw
