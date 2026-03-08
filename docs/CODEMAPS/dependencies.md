<!-- Generated: 2026-03-08 | Files scanned: 236 | Token estimate: ~220 -->
# Dependencies Codemap

**Entry Points:** /home/crasleepish/dev/finance-data-fetcher/pyproject.toml

## Architecture
```
pyproject.toml
   +--> [project.dependencies] (runtime)
   +--> [project.optional-dependencies.dev] (developer tools)
   +--> [tool.*] sections configuring linters/type checkers
```

## Key Modules
| Module | Purpose | Exports | Dependencies |
| --- | --- | --- | --- |
| pyproject.toml | Defines project metadata, runtime dependencies, dev deps, and tooling configuration (ruff, mypy, pytest) | `finance-data-fetcher` project metadata, dependency lists | uv build backend reads this file |
| docs/arc_diag.dot | Architecture diagram referenced from README and codemaps; not imported but maintained for design docs | DOT graph | None |

## Data Flow
1. Runtime dependencies in `[project.dependencies]` (fastapi, uvicorn, pydantic, SQLAlchemy, PyYAML, psycopg2-binary, requests, tushare, akshare, pysnowball, pandas, numpy, scikit-learn, numba, vectorbt, tqdm) power the API, services, infra, and data processing pipelines.
2. Dev dependencies configured under `[project.optional-dependencies.dev]` provide `mypy`, `pytest`, `ruff`, `httpx`, `testcontainers`, plus typing stubs.
3. Tooling sections ensure consistent formatting/linting (ruff) and type checking (mypy) for the entire repo, aligning developer workflows with project standards.

## External Dependencies
- `fastapi`, `uvicorn`, `pydantic`, `SQLAlchemy`, `PyYAML`, `psycopg2-binary`, `requests`, `tushare`, `akshare`, `pysnowball`, `pandas`, `numpy`, `scikit-learn`, `numba`, `vectorbt`, `tqdm`
- `mypy`, `pytest`, `ruff`, `httpx`, `testcontainers`, `types-requests`, `types-PyYAML`

## Related Areas
- `architecture.md` — shows how dependencies flow through API/services/infra
- `data.md` — ties dependencies to data persistence concerns (SQLAlchemy, pandas)
