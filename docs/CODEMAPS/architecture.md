<!-- Generated: 2026-03-08 | Files scanned: 236 | Token estimate: ~350 -->
# Architecture Codemap

**Entry Points:** /home/crasleepish/dev/finance-data-fetcher/src/api/main.py, /home/crasleepish/dev/finance-data-fetcher/run.sh, /home/crasleepish/dev/finance-data-fetcher/config/app.yaml

## Architecture
```
              +---------------------+
              | FastAPI HTTP layer  |
              | (src/api/main.py)   |
              +----------+----------+
                         |
               +---------v---------+
               | services/task_    |
               | service & calendar|
               +---------+---------+
                         |
               +---------v---------+
               | Workflow Engine   |
               | (PipelineRegistry,
               |  PipelineSelector)|
               +---------+---------+
                         |
          +--------------v----------------+
          | infra (db engine/repository,  |
          | fetchers, guard, logging)     |
          +--------------+----------------+
                         |
                +--------v--------+
                | core/business   |
                | logic (pipelines|
                | fetch/clean)    |
                +-----------------+
                         |
                +------------------+
                | External clients |
                | (Tushare, Akshare|
                |  adapters)       |
                +------------------+
```

## Key Modules
| Module | Purpose | Exports | Dependencies |
| --- | --- | --- | --- |
| src/api/main.py | App factory that wires calendars, pipelines, worker threads, repos, and routers | `create_app`, `app` | `fastapi`, `services`, `infra`, `config`, `core` |
| src/services/workflow_engine.py | Orchestrates task lifecycle: stores, pipeline registry, selectors, idempotency | `WorkflowEngine` | `infra.task_state`, `services.pipeline_selector`, `core.pipeline` |
| src/core/pipeline/registry.py | Registers pipelines and exposes lookup API consumed by workers | `PipelineRegistry` | `core.pipeline.pipeline`, `services` |
| src/infra/worker_runtime/runtime.py | Manages background worker loop, polling task queue, invoking pipelines | `WorkerRuntime` | `infra.queue`, `infra.task_state`, `services.workflow_engine` |

## Data Flow
Requests hit `/tasks` routers in `src/api/routers/tasks.py`, are validated via Pydantic, then translated into `PipelineTask` payloads. `TaskService` applies idempotency guards, enqueues work, and persists status via `TaskStatusStore`. `WorkerRuntime` dequeues work, uses `PipelineRegistry` to resolve the registered pipeline (e.g., `rt_index_hist_xueqiu`), and amplifies the `WorkflowEngine` that pushes cleaned payloads into `infra.db.Repository` tables defined in `src/infra/db/tables.py`.

## External Dependencies
- `fastapi` - HTTP layer and lifespan hooks
- `uvicorn` - ASGI server invoked via `run.sh`
- `sqlalchemy` - Core engine, `Table`, transactions, `Repository`
- `tushare`, `akshare`, `pysnowball` - Data sources for fetchers
- `requests`, `httpx` (via optional dependencies) - HTTP fetch helpers

## Related Areas
- `backend.md` — drill into pipeline/service layer responsibilities
- `frontend.md` — documents the HTTP endpoints and routers
- `data.md` — traces DB tables and config-driven data targets
- `dependencies.md` — lists runtime and dev dependencies