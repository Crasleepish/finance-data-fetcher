from __future__ import annotations

import time
from dataclasses import dataclass, field

from sqlalchemy.engine import Engine

from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.registry import PipelineRegistry
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from core.workflow.failover import FetchCleanFailoverPolicy
from infra.db.repository import Repository
from infra.db.tables import test_messages
from infra.idempotency.guard import IdempotencyGuard
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.worker_runtime.runtime import WorkerRuntime
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState
from services.pipeline_selector import PipelineSelector
from services.task_service import TaskService
from services.workflow_engine import WorkflowEngine


@dataclass
class DummyPipeline(IngestionPipeline):
    planned: list[ChunkArgs] = field(default_factory=list)
    fetched: list[ChunkArgs] = field(default_factory=list)
    cleaned: int = 0
    _counter: int = 0

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        self.planned = [
            {"cursor": "a", "params": arguments.get("params", {})},
            {"cursor": "b", "params": arguments.get("params", {})},
        ]
        return list(self.planned)

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        self.fetched.append(chunk_args)
        return [{"cursor": chunk_args["cursor"]}]

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        self.cleaned += 1
        normalized: list[dict[str, object]] = []
        for row in raw_batch:
            self._counter += 1
            normalized.append({"id": self._counter, "message": str(row["cursor"])})
        return normalized


def test_pipeline_runs_via_worker(postgres_engine: Engine) -> None:
    registry = PipelineRegistry()
    pipeline = DummyPipeline()
    registry.register("dummy", pipeline)

    store = TaskStatusStore(engine=postgres_engine)
    queue = InMemoryTaskQueue()
    guard = IdempotencyGuard(engine=postgres_engine)
    service = TaskService(store=store, queue=queue, guard=guard)
    selector = PipelineSelector(mapping={TaskSpec.PIPELINE: ["dummy"]})
    repo = Repository(engine=postgres_engine, table=test_messages)
    workflow = WorkflowEngine(
        store=store,
        registry=registry,
        selector=selector,
        repo=repo,
        failover=FetchCleanFailoverPolicy(),
    )
    runtime = WorkerRuntime(queue=queue, store=store, handler=workflow)
    runtime.start()

    task = service.start_task(
        PipelineTask(
            spec=TaskSpec.PIPELINE,
            pipeline_id="dummy",
            source="unit-test",
            task_type="dummy",
            arguments={"params": {"start_date": "2024-01-01"}},
            options={"chunk_size": 2},
        )
    )

    deadline = time.time() + 10
    while time.time() < deadline:
        record = store.get_by_id(task.task_id)
        if record.state == TaskState.SUCCEEDED:
            runtime.stop()
            assert pipeline.planned
            assert pipeline.fetched
            assert pipeline.cleaned == len(pipeline.planned)
            return
        time.sleep(0.2)

    runtime.stop()
    raise AssertionError("pipeline task did not complete")
