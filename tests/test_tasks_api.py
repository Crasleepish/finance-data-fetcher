from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from api.routers.tasks import router as tasks_router
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
