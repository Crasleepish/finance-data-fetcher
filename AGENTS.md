# AGENTS.md

High-signal guidance for working in this repo. Consult the nearest scoped `AGENTS.md` before editing under `src/`.

## Runtime

- Python 3.12. Install with dev deps: `uv sync --dev`.
- Local server: `./run.sh --reload --host 0.0.0.0 --port 8000`
  (`run.sh` wraps `uv run uvicorn api.main:app --app-dir src`).
- App entrypoint: `src/api/main.py` — `create_app()` builds the FastAPI app; module-level `app` is the ASGI target.

## Quality gates (no CI is configured — run manually)

```bash
uv run ruff check .
uv run ruff format .
uv run mypy src/
uv run pytest
```

Focused test: `uv run pytest tests/test_config_loader.py::test_name`.

## Architecture

- Dependency direction: `api → services → core`; `services` may also call `infra`. Lower layers must not import higher layers.
- `api`: FastAPI routers only. `services`: orchestration/workflows. `core`: framework-agnostic business logic. `infra`: DB, logging, external clients. `models`: Pydantic schemas.
- Pipelines are registered explicitly in `create_app()` (lifespan); the runtime spec → pipeline mapping lives in `config/task_pipeline_mapping.py`.
- The worker runs in-process against an in-memory queue (`src/infra/queue/in_memory.py`): pending queued tasks are lost on restart. Task state persists in Postgres.

## Config

- Centralized in `src/config/loader.py`; reads `config/app.yaml` (gitignored — start from `config/app.yaml.template`).
- Override via `APP_*` env vars (e.g. `APP_DB_URL`, `APP_TUSHARE_TOKEN_PRIVATE`, `APP_TUSHARE_TOKEN_PUBLIC`).
- Business logic must not read env vars directly. Never leak tokens/secrets — including in logs (use `logging`, never `print`).

## Database

- Prefer SQLAlchemy Core: `Table`/`Column` with `select/insert/update` and explicit transactions. No hidden ORM side effects.
- Local dev DB access: connect to PostgreSQL at `127.0.0.1:5432`; the host in `config/app.yaml` is for container-internal access. Username, password, and database name are the same in both.
- For manual database debugging, use `psql`. Read credentials from `config/app.yaml` first, then `DB_USER` / `DB_PASSWORD`; if neither source provides them, ask the user.

## Testing

- Unit tests: no real DB, no network. Integration tests use `TestClient` plus testcontainers Postgres (`tests/conftest.py`); they auto-skip when Docker is unavailable.

## Documentation updates

Update this file when architecture/core rules, runtime, or execution environment change, or repeated user intent (≥3 times) warrants a project rule. Show the exact suggested wording for the change.

## Uncertainty rule

Before writing or modifying code, check for genuine uncertainty: unclear requirements, multiple reasonable interpretations, or missing constraints. If any exist — STOP, list the uncertainties, and ask the user; wait for confirmation. Do not assume.
