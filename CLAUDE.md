# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

- Build a maintainable microservice for data fetch → clean → persist.
- Tech stack: FastAPI, SQLAlchemy (prefer Core), Pydantic, uv.
- Architecture diagram: `docs/arc_diag.dot`.

## Common commands

```bash
uv sync --dev
./run.sh --reload --host 0.0.0.0 --port 8000
uv run ruff check .
uv run ruff format .
uv run mypy src/
uv run pytest
# run a single test
uv run pytest tests/test_config_loader.py::test_name
```

## Architecture & boundaries

- Layering: `api → services → core`; services may call `infra`. Lower layers must not import higher layers.
- Top-level responsibilities:
  - api: FastAPI endpoints only
  - services: orchestration and workflows
  - core: framework-agnostic business logic
  - infra: DB, logging, external clients
  - models: Pydantic schemas (and ORM models if needed)
  - config: centralized configuration
- Pipeline routing is defined in `config/task_pipeline_mapping.py` (spec → pipeline implementation).
- Configuration is loaded by `src/config/loader.py` from `config/app.yaml` or `APP_CONFIG_PATH`, with env overrides (e.g., `APP_DB_URL`, `APP_TUSHARE_TOKEN_PRIVATE`, `APP_TUSHARE_TOKEN_PUBLIC`). Business logic must not read env vars directly.

## Implementation constraints (from AGENTS.md)

- Prefer SQLAlchemy Core (`Table`, `Column`, `select/insert/update`) with explicit transactions.
- No hidden logic: no logic in `__init__.py`, no side effects at import time, and no implicit global state mutation.

## Testing contract

- Tests live under `tests/` with `test_*.py` and `test_*` functions.
- Unit tests: no real DB, no network, no shared external state.
- Integration tests: real database, external APIs mocked, FastAPI tested via `TestClient`.

## Document updates

Update `AGENTS.md` when architecture/core rules change, new modules are introduced, runtime changes occur, or repeated user intent (>=3 times) warrants a project rule. Provide exact suggested wording for the update.
