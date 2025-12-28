from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine

from api.main import create_app
from config.loader import load_config
from core.clean.csv_cleaner import CsvMessageCleaner
from core.fetch.errors import RetryableError
from core.fetch.retry import RetryPolicy
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.db.tables import metadata, task_table, test_messages
from infra.fetcher.csv_fetcher import CsvFetcher
from models.task_spec import TaskSpec
from models.task_status import TaskState

_CSV_FILE_NAME = "integration_messages.csv"
_MESSAGE_PREFIX = "integration_csv_test"


@pytest.fixture(scope="session")
def real_db_engine() -> Engine:
    config = load_config()
    engine = create_engine(config.database.url, pool_pre_ping=True)
    metadata.create_all(engine, tables=[task_table, test_messages])
    yield engine
    engine.dispose()


@dataclass
class CsvRetryPipeline(IngestionPipeline):
    csv_path: Path
    retry_policy: RetryPolicy = field(
        default_factory=lambda: RetryPolicy(
            max_attempts=2,
            base_delay=0,
            max_delay=0,
            jitter_ratio=0,
            sleep_fn=lambda _: None,
            random_fn=lambda: 0,
        )
    )
    _fetcher: CsvFetcher = field(default_factory=CsvFetcher, init=False)
    _cleaner: CsvMessageCleaner = field(default_factory=CsvMessageCleaner, init=False)
    _attempts: dict[str, int] = field(default_factory=dict, init=False)

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        params = arguments.get("params", {})
        csv_path = params.get("csv_path")
        if not isinstance(csv_path, str):
            raise ValueError("csv_path is required")
        return [
            {"cursor": "chunk-1", "params": {"csv_path": csv_path}},
            {"cursor": "chunk-2", "params": {"csv_path": csv_path}},
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        cursor = str(chunk_args.get("cursor", ""))

        def operation() -> RawBatch:
            attempt = self._attempts.get(cursor, 0) + 1
            self._attempts[cursor] = attempt
            if cursor == "chunk-1" and attempt == 1:
                raise RetryableError("temporary 500")
            return self._fetcher.fetch(chunk_args)

        return self.retry_policy.execute(operation)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        time.sleep(0.2)
        normalized = self._cleaner.clean(raw_batch)
        if not normalized:
            return normalized
        if len(normalized) == 1:
            return normalized
        return normalized[:1] if self._attempts.get("chunk-2", 0) == 0 else normalized[1:]


def _write_csv(path: Path, rows: list[tuple[int, str]]) -> None:
    payload = ["id,message"]
    payload.extend(f"{record_id},{message}" for record_id, message in rows)
    path.write_text("\n".join(payload) + "\n", encoding="utf-8")


def _next_message_id(engine: Engine, step: int = 10) -> int:
    with engine.begin() as connection:
        current_max = connection.execute(select(func.max(test_messages.c.id))).scalar()
    return int(current_max or 0) + step


def _wait_for_completion(client: TestClient, task_id: int, timeout_s: float) -> list[Decimal]:
    deadline = time.time() + timeout_s
    progress_samples: list[Decimal] = []
    observed_running = False
    while time.time() < deadline:
        status_response = client.get(f"/tasks/{task_id}")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        state = status_payload["state"]
        progress_samples.append(Decimal(status_payload["progress"]))
        if state == TaskState.RUNNING.value:
            running_response = client.get("/tasks/running")
            assert running_response.status_code == 200
            observed_running = any(
                item["task_id"] == task_id for item in running_response.json()
            ) or observed_running
        if state == TaskState.SUCCEEDED.value:
            assert observed_running
            return progress_samples
        time.sleep(0.1)
    raise AssertionError("task did not complete in time")


def test_real_db_full_chain_with_csv_pipeline(real_db_engine: Engine) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    csv_path = repo_root / _CSV_FILE_NAME

    base_id = _next_message_id(real_db_engine)
    rows = [
        (base_id, f"{_MESSAGE_PREFIX}_run1_a"),
        (base_id + 1, f"{_MESSAGE_PREFIX}_run1_b"),
    ]
    _write_csv(csv_path, rows)

    app = create_app()
    with TestClient(app) as client:
        pipeline = CsvRetryPipeline(csv_path=csv_path)
        app.state.pipeline_registry.register("dummy", pipeline)

        payload = {
            "spec": TaskSpec.PIPELINE,
            "pipeline_id": "dummy",
            "source": "integration-test",
            "task_type": "csv",
            "arguments": {"params": {"csv_path": str(csv_path)}},
            "options": {},
        }

        start_response = client.post("/tasks/start", json=payload)
        assert start_response.status_code == 200
        first_task_id = start_response.json()["task_id"]

        dedupe_response = client.post("/tasks/start", json=payload)
        assert dedupe_response.status_code == 200
        assert dedupe_response.json()["task_id"] == first_task_id

        progress_samples = _wait_for_completion(client, first_task_id, timeout_s=10)
        assert progress_samples[-1] == Decimal("100")
        assert any(sample > Decimal("0") for sample in progress_samples)
        assert pipeline._attempts.get("chunk-1") == 2

        with real_db_engine.begin() as connection:
            stored = connection.execute(
                select(test_messages.c.id, test_messages.c.message).where(
                    test_messages.c.id.in_([row[0] for row in rows])
                )
            ).all()
        assert {row.id for row in stored} == {row[0] for row in rows}

        base_id = _next_message_id(real_db_engine)
        rerun_rows = [
            (base_id, f"{_MESSAGE_PREFIX}_run2_a"),
            (base_id + 1, f"{_MESSAGE_PREFIX}_run2_b"),
        ]
        _write_csv(csv_path, rerun_rows)

        pipeline = CsvRetryPipeline(csv_path=csv_path)
        app.state.pipeline_registry.register("dummy", pipeline)

        rerun_response = client.post("/tasks/start", json=payload)
        assert rerun_response.status_code == 200
        rerun_task_id = rerun_response.json()["task_id"]
        assert rerun_task_id != first_task_id

        progress_samples = _wait_for_completion(client, rerun_task_id, timeout_s=10)
        assert progress_samples[-1] == Decimal("100")

        with real_db_engine.begin() as connection:
            stored = connection.execute(
                select(test_messages.c.id, test_messages.c.message).where(
                    test_messages.c.id.in_([row[0] for row in rerun_rows])
                )
            ).all()
        assert {row.id for row in stored} == {row[0] for row in rerun_rows}
