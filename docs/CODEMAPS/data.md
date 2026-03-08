<!-- Generated: 2026-03-08 | Files scanned: 236 | Token estimate: ~360 -->
# Data Codemap

**Entry Points:** /home/crasleepish/dev/finance-data-fetcher/config/app.yaml, /home/crasleepish/dev/finance-data-fetcher/src/infra/db/tables.py, /home/crasleepish/dev/finance-data-fetcher/src/infra/db/repository.py, /home/crasleepish/dev/finance-data-fetcher/src/services/workflow_engine.py

## Architecture
```
            config/app.yaml
                  |
        +---------v---------+          +---------------------+
        | pipeline registry |--------->| src/services        |
        | (PipelineSelector)|          | workflow_engine.py  |
        +---------+---------+          +----------+----------+
                  |                            |
            +-----v------+           +---------v----------+
            | fetch/clean |---------->| Repository layer   |
            | pipelines   |           | (infra/db/repository)|
            +------------+           +---------+----------+
                                          |
                               +----------v-----------+
                               | src/infra/db/tables.py|
                               | (SQLAlchemy metadata) |
                               +-----------------------+
```

## Key Modules
| Module | Purpose | Exports | Dependencies |
| --- | --- | --- | --- |
| config/app.yaml | Centralizes database, Tushare/Akshare, gold, logging, and data sets (index/fund) used by pipelines | YAML sections `database`, `data`, `gold`, `logging` | `models.task_spec`, `services.pipeline_selector` |
| src/infra/db/tables.py | SQLAlchemy Core schema for task tracking, market data, and gold reports | `Table` definitions for `task_table`, `stock_hist_unadj`, `index_hist`, `fund_hist`, etc. | `sqlalchemy`, `sqlalchemy.dialects.postgresql`, `core` components |
| src/infra/db/repository.py | Thin repository helpers for insert/upsert/replace with explicit transactions | `Repository` class | `sqlalchemy`, `src/infra/db/tables.py`, `typing` |
| src/services/workflow_engine.py | Persists cleaned batches into `Repository`, coordinates upsert/replace rules per pipeline | `WorkflowEngine`, `StageError`, `CancelledError` | `core.pipeline`, `infra.db.repository`, `infra.task_state.store`, `models` |

## Data Flow
1. Pipelines (e.g., `stock_hist_unadj`, `index_hist_stock`, `fundamental_data`) request config-driven chunk arguments (stock codes, date ranges, gold symbols) defined in `config/app.yaml`.
2. Each pipeline emits batches that `WorkflowEngine` sends to `Repository.insert_batch`, `Repository.upsert_batch`, or `Repository.replace_all` depending on `upsert_keys_by_pipeline` and streaming feeds marked in `replace_by_pipeline`.
3. Repository writes go into SQLAlchemy tables (`stock_hist_unadj`, `fundamental_data`, `index_hist`, `gold_cftc_report`, `gold_future_curve`, `fund_beta`, `market_factors`, `rt_*` tables) defined in `src/infra/db/tables.py` with indexed columns optimized for queries (e.g., `stock_code` + `date`).
4. Task status tracking happens parallel via `task_table` and `test_messages`; progress/errors recorded by `TaskStatusStore` in `infra/task_state/store.py`.

## External Dependencies
- `sqlalchemy` / `psycopg2-binary` – engine, metadata, `Table`, transactions, upsert helpers
- `pandas` / `numpy` / `vectorbt` – used inside cleaning/factor modules (implied by `core/clean` modules)
- `tushare`, `akshare`, `pysnowball` – data sources populating tables via fetchers
- `requests` / `httpx` – HTTP clients used by fetchers and calendar sync (optional dependencies)

## Related Areas
- `architecture.md` — overall system wiring, linking API, services, infra
- `backend.md` — flow from `/tasks` to `WorkflowEngine` and pipelines
- `dependencies.md` — maps package dependencies powering tables and fetchers