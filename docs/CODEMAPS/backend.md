<!-- Generated: 2026-03-08 | Files scanned: 236 | Token estimate: ~380 -->
# Backend Codemap

**Entry Points:** /home/crasleepish/dev/finance-data-fetcher/src/services/task_service.py, /home/crasleepish/dev/finance-data-fetcher/src/services/workflow_engine.py, /home/crasleepish/dev/finance-data-fetcher/src/services/pipeline_selector.py, /home/crasleepish/dev/finance-data-fetcher/src/core/pipeline/registry.py

## Architecture
```
                +--------------------------+
                | TaskService (API layer)  |
                | - ensures idempotency    |
                | - creates TaskStatus     |
                +-----------+--------------+
                            |
                +-----------v--------------+
                | TaskStatusStore + Queue   |
                +-----------+--------------+
                            |
                +-----------v--------------+
                | WorkflowEngine            |
                | - selects pipeline        |
                | - runs fetch/clean/persist|
                +--+-------------------+---+
                   |                   |
       +-----------v---+         +-----v-----------------+
       | PipelineRegistry|         | failover policy        |
       | (core/pipeline) |         | selects alternate pipe |
       +-----------------+         +-----------------------+
```

## Key Modules
| Module | Purpose | Exports | Dependencies |
| --- | --- | --- | --- |
| src/services/task_service.py | Entry point for POST /tasks/start; applies idempotency, persists TaskStatusRecord, enqueues work | `TaskService` | `infra.task_state.store`, `infra.queue`, `infra.idempotency.guard`, `models` |
| src/services/workflow_engine.py | Executes pipelines chunk-by-chunk, handles fetch/clean/persist, manages failover and cancel | `WorkflowEngine` | `core.pipeline.registry`, `services.pipeline_selector`, `models`, `infra.db.repository`, `infra.task_state.store` |
| src/services/pipeline_selector.py | Loads `config/task_pipeline_mapping.py` and provides spec → pipeline candidates | `PipelineSelector`, `load_pipeline_mapping` | `models.task_spec` |
| src/core/pipeline/registry.py | Runtime registry for pipeline implementations | `PipelineRegistry` | `typing`, `core.pipeline.pipeline` |
| src/infra/worker_runtime/runtime.py | Background loop that dequeues TaskItems and calls WorkflowEngine.handle | `WorkerRuntime` | `infra.task_state.store`, `infra.queue`, `services.workflow_engine` |
| src/infra/db/repository.py | SQLAlchemy Core repository helpers used by pipelines for insert/upsert/replace | `Repository` | `sqlalchemy`, `infra.db.tables` |

## Data Flow
1. `TaskService.start_task` validates `PipelineTask`, applies the `IdempotencyGuard`, and stores a `TaskStatusRecord` with JSON payload from `models.task_payload`.
2. A `TaskItem` lands in the in-memory queue; `WorkerRuntime` polls the queue and invokes `WorkflowEngine.handle` with the pipeline id selected by `PipelineSelector` and resolved via `PipelineRegistry`.
3. Within `WorkflowEngine`, each chunk triggers `fetch`, `clean`, and `persist` phases. Persistence writes to the `Repository` tied to `infra/db/tables` using upsert keys defined per pipeline and may replace existing data for real-time feeds.
4. Failover policies in `core.workflow.failover` select an alternate pipeline if a chunk fails, while `TaskStatusStore` tracks state, progress, and errors inside Postgres-backed tables.

## External Dependencies
- `sqlalchemy` + `psycopg2-binary` for Core engine, metadata, and transactions
- `tushare`, `akshare`, `pysnowball` fetch clients under `infra.fetcher`
- `requests`/`httpx` for HTTP helpers used by fetchers and calendars
- `fastapi`/`uvicorn` indirectly through the API layer that starts these backend services

## Related Areas
- `architecture.md` — high-level system view linking API, services, infra
- `data.md` — how TaskStatus and domain tables are structured
- `dependencies.md` — runtime dependencies that power these modules
