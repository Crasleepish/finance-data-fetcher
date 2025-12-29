from __future__ import annotations

import time
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from api.main import create_app
from config.loader import load_config
from models.task_spec import TaskSpec
from models.task_status import TaskState


@pytest.fixture(scope="session")
def real_db_engine() -> Engine:
    config = load_config()
    engine = create_engine(config.database.url, pool_pre_ping=True)
    yield engine
    engine.dispose()


def _wait_for_completion(client: TestClient, task_id: int, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    observed_running = False
    while time.time() < deadline:
        status_response = client.get(f"/tasks/{task_id}")
        assert status_response.status_code == 200
        status_payload = status_response.json()
        state = status_payload["state"]
        progress = Decimal(status_payload["progress"])
        if state == TaskState.RUNNING.value:
            running_response = client.get("/tasks/running")
            assert running_response.status_code == 200
            observed_running = (
                any(item["task_id"] == task_id for item in running_response.json())
                or observed_running
            )
        if state == TaskState.FAILED.value:
            raise AssertionError(f"task failed: {status_payload.get('error')}")
        if state == TaskState.CANCELLED.value:
            raise AssertionError("task cancelled")
        if state == TaskState.SUCCEEDED.value:
            assert observed_running
            assert progress == Decimal("100")
            return
        time.sleep(0.5)
    raise AssertionError("task did not complete in time")


def test_get_stock_info_pipeline_integration(real_db_engine: Engine) -> None:
    with real_db_engine.begin() as connection:
        exists = connection.execute(
            text(
                "select 1 from information_schema.tables where table_name = 'stock_info' limit 1"
            )
        ).scalar()
    assert exists == 1

    app = create_app()
    with TestClient(app) as client:
        payload = {
            "spec": TaskSpec.GET_STOCK_INFO,
            "pipeline_id": "stock_info",
            "source": "integration-test",
            "task_type": "stock_info",
            "arguments": {
                "params": {
                    "exchange": "",
                    "list_statuses": ["L", "D", "P"],
                }
            },
            "options": {},
        }

        start_response = client.post("/tasks/start", json=payload)
        assert start_response.status_code == 200
        task_id = start_response.json()["task_id"]

        _wait_for_completion(client, task_id, timeout_s=60)
