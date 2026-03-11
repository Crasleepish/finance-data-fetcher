from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from api.routers.tasks import router as tasks_router
from infra.db.tables import task_table
from infra.idempotency.guard import IdempotencyGuard
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_spec import TaskSpec
from services.task_service import TaskService


def _build_test_app(engine: Engine) -> FastAPI:
    app = FastAPI()
    store = TaskStatusStore(engine=engine)
    queue = InMemoryTaskQueue()
    guard = IdempotencyGuard(engine=engine)
    app.state.task_store = store
    app.state.task_service = TaskService(store=store, queue=queue, guard=guard)
    app.include_router(tasks_router)
    return app


def test_start_and_get_task(postgres_engine: Engine) -> None:
    app = _build_test_app(postgres_engine)
    client = TestClient(app)

    payload = {
        "spec": TaskSpec.NOOP_SLEEP,
        "pipeline_id": "demo",
        "source": "unit-test",
        "task_type": "noop",
        "arguments": {},
        "options": {},
    }

    response = client.post("/tasks/start", json=payload)
    assert response.status_code == 200
    data = response.json()
    task_id = data["task_id"]

    status_response = client.get(f"/tasks/{task_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["task_id"] == task_id
    assert status_data["state"] in {"PENDING", "RUNNING"}


def test_list_running(postgres_engine: Engine) -> None:
    app = _build_test_app(postgres_engine)
    client = TestClient(app)

    payload = {
        "spec": TaskSpec.NOOP_SLEEP,
        "pipeline_id": "demo",
        "source": "unit-test",
        "task_type": "noop",
        "arguments": {},
        "options": {},
    }

    start_response = client.post("/tasks/start", json=payload)
    assert start_response.status_code == 200
    task_id = start_response.json()["task_id"]

    running_response = client.get("/tasks/running")
    assert running_response.status_code == 200
    running = running_response.json()
    assert any(item["task_id"] == task_id for item in running)


def test_start_stock_info_example_payload(postgres_engine: Engine) -> None:
    app = _build_test_app(postgres_engine)
    client = TestClient(app)

    payload = {
        "spec": TaskSpec.GET_STOCK_INFO,
        "pipeline_id": "stock_info",
        "source": "manual",
        "task_type": "stock_info",
        "arguments": {
            "params": {
                "exchange": "",
                "list_statuses": ["L", "D", "P"],
            }
        },
        "options": {},
    }

    response = client.post("/tasks/start", json=payload)
    assert response.status_code == 200
    data = response.json()
    task_id = data["task_id"]

    status_response = client.get(f"/tasks/{task_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["spec"] == "get_stock_info"


def test_start_stock_hist_unadj_payload(postgres_engine: Engine) -> None:
    app = _build_test_app(postgres_engine)
    client = TestClient(app)

    payload = {
        "spec": TaskSpec.GET_STOCK_HIST_UNADJ,
        "pipeline_id": "stock_hist_unadj",
        "source": "manual",
        "task_type": "stock_hist_unadj",
        "arguments": {"params": {"start_date": "2024-01-02", "end_date": "2024-01-02"}},
        "options": {},
    }

    response = client.post("/tasks/start", json=payload)
    assert response.status_code == 200


def test_list_tasks_filters_and_pagination(postgres_engine: Engine) -> None:
    app = _build_test_app(postgres_engine)
    client = TestClient(app)

    first_payload = {
        "spec": TaskSpec.NOOP_SLEEP,
        "pipeline_id": "demo",
        "source": "unit-test",
        "task_type": "noop",
        "arguments": {},
        "options": {},
    }
    second_payload = {
        "spec": TaskSpec.NOOP_SLEEP,
        "pipeline_id": "demo",
        "source": "unit-test-2",
        "task_type": "noop",
        "arguments": {},
        "options": {},
    }

    first_response = client.post("/tasks/start", json=first_payload)
    assert first_response.status_code == 200
    first_task_id = first_response.json()["task_id"]

    second_response = client.post("/tasks/start", json=second_payload)
    assert second_response.status_code == 200
    second_task_id = second_response.json()["task_id"]
    assert second_task_id != first_task_id

    query_response = client.get("/tasks/list", params={"task_id": first_task_id})
    assert query_response.status_code == 200
    payload = query_response.json()
    assert payload["meta"]["total"] == 1
    assert payload["items"][0]["task_id"] == first_task_id
    assert "task_payload" in payload["items"][0]

    spec_response = client.get("/tasks/list", params={"spec": "noop_sleep"})
    assert spec_response.status_code == 200
    spec_payload = spec_response.json()
    assert spec_payload["meta"]["total"] >= 2

    state_response = client.get(
        "/tasks/list",
        params=[("state", "PENDING"), ("state", "RUNNING")],
    )
    assert state_response.status_code == 200
    state_payload = state_response.json()
    assert state_payload["meta"]["total"] >= 2

    page_response = client.get("/tasks/list", params={"page": 1, "page_size": 1})
    assert page_response.status_code == 200
    page_payload = page_response.json()
    assert page_payload["meta"]["page"] == 1
    assert page_payload["meta"]["page_size"] == 1
    assert len(page_payload["items"]) == 1


def test_list_tasks_datetime_ranges_and_ordering(postgres_engine: Engine) -> None:
    app = _build_test_app(postgres_engine)
    client = TestClient(app)

    first_payload = {
        "spec": TaskSpec.NOOP_SLEEP,
        "pipeline_id": "demo",
        "source": "unit-test",
        "task_type": "noop",
        "arguments": {},
        "options": {},
    }
    second_payload = {
        "spec": TaskSpec.NOOP_SLEEP,
        "pipeline_id": "demo",
        "source": "unit-test-2",
        "task_type": "noop",
        "arguments": {},
        "options": {},
    }

    first_response = client.post("/tasks/start", json=first_payload)
    assert first_response.status_code == 200
    first_task_id = first_response.json()["task_id"]

    second_response = client.post("/tasks/start", json=second_payload)
    assert second_response.status_code == 200
    second_task_id = second_response.json()["task_id"]

    first_created = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    first_started = datetime(2024, 1, 1, 0, 10, 0, tzinfo=timezone.utc)
    first_finished = datetime(2024, 1, 1, 0, 20, 0, tzinfo=timezone.utc)
    first_heartbeat = datetime(2024, 1, 1, 0, 15, 0, tzinfo=timezone.utc)
    second_created = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    second_started = datetime(2024, 1, 2, 0, 10, 0, tzinfo=timezone.utc)
    second_finished = datetime(2024, 1, 2, 0, 20, 0, tzinfo=timezone.utc)
    second_heartbeat = datetime(2024, 1, 2, 0, 15, 0, tzinfo=timezone.utc)

    with postgres_engine.begin() as connection:
        connection.execute(
            task_table.update()
            .where(task_table.c.task_id == first_task_id)
            .values(
                created_at=first_created,
                started_at=first_started,
                finished_at=first_finished,
                last_heartbeat_at=first_heartbeat,
            )
        )
        connection.execute(
            task_table.update()
            .where(task_table.c.task_id == second_task_id)
            .values(
                created_at=second_created,
                started_at=second_started,
                finished_at=second_finished,
                last_heartbeat_at=second_heartbeat,
            )
        )

    range_response = client.get(
        "/tasks/list",
        params={
            "created_at_from": "2024-01-02T00:00:00+00:00",
            "created_at_to": "2024-01-02T23:59:59+00:00",
        },
    )
    assert range_response.status_code == 200
    range_payload = range_response.json()
    assert range_payload["meta"]["total"] == 1
    assert range_payload["items"][0]["task_id"] == second_task_id

    heartbeat_response = client.get(
        "/tasks/list",
        params={
            "last_heartbeat_at_from": "2024-01-01T00:00:00+00:00",
            "last_heartbeat_at_to": "2024-01-01T23:59:59+00:00",
        },
    )
    assert heartbeat_response.status_code == 200
    heartbeat_payload = heartbeat_response.json()
    assert heartbeat_payload["meta"]["total"] == 1
    assert heartbeat_payload["items"][0]["task_id"] == first_task_id

    ordering_response = client.get("/tasks/list", params={"page": 1, "page_size": 2})
    assert ordering_response.status_code == 200
    ordering_payload = ordering_response.json()
    assert ordering_payload["items"][0]["task_id"] == second_task_id
    assert ordering_payload["items"][1]["task_id"] == first_task_id
