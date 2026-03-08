<!-- Generated: 2026-03-08 | Files scanned: 236 | Token estimate: ~280 -->
# Frontend Codemap

**Entry Points:** /home/crasleepish/dev/finance-data-fetcher/src/api/main.py, /home/crasleepish/dev/finance-data-fetcher/src/api/routers/tasks.py, /home/crasleepish/dev/finance-data-fetcher/src/api/routers/calendar.py

## Architecture
```
Browser/CLI -> uvicorn (run.sh) -> FastAPI app (src/api/main.py)
                       |           |
                       |           +--> routers/tasks.py (/tasks endpoint)
                       |           +--> routers/calendar.py (/calendar endpoint)
                       |           |
                       +--> Calendars + TaskService wired via app.state
```

## Key Modules
| Module | Purpose | Exports | Dependencies |
| --- | --- | --- | --- |
| src/api/main.py | Lifespan-managed FastAPI app factory that wires logging, calendar service, queue, registry, worker runtime, and routers | `create_app`, `app` | `fastapi`, `services`, `infra`, `config`, `core` |
| src/api/routers/tasks.py | Task lifecycle endpoints: start, list running, status, cancel; exposes structured responses and error handling | `router`, task response models | `fastapi`, `models.task_payload`, `services.task_service`, `infra.task_state.store` |
| src/api/routers/calendar.py | Calendar sync endpoint for manually seeding trade calendars | `router`, `CalendarSyncRequest`, `CalendarSyncResponse` | `fastapi`, `services.calendar_service` |

## Data Flow
1. Clients send POST `/tasks/start` with a `PipelineTask`; FastAPI validates payload with `models.task_payload.PipelineTask` and ensures idempotency digestable arguments.
2. `TaskService` creates a `TaskStatusRecord`, enqueues work, and returns a minimal `TaskStartResponse` with task id and state.
3. GET `/tasks/running` and `/tasks/{task_id}` read task state from `TaskStatusStore` and translate records into response models.
4. POST `/tasks/cancel/{task_id}` routes cancellation through `TaskService.cancel_task` which updates state and optionally removes pending items from the queue.
5. POST `/calendar/sync` delegates to `CalendarService.sync` and responds with inserted row counts.

## External Dependencies
- `fastapi` / `uvicorn` – HTTP server and routing layer (factory in `run.sh`).
- `pydantic` – request/response validation and `PipelineTask` schema.

## Related Areas
- `backend.md` — how `/tasks` translates into workflows, queues, and persistence.
- `architecture.md` — overall service wiring between API, services, and infra.