# finance-data-fetcher

## 0. Project Intent

* Build a **maintainable microservice** for
  **data fetch → clean → persist**
* Tech stack:

  * FastAPI
  * **SQLAlchemy (prefer Core)**
  * Pydantic
  * uv
* Core values:

  * Explicit contracts
  * Deterministic behavior
  * Testability
  * Long-term maintainability
  
* Refer to the architecture diagram `docs/arc_diag.dot` when necessary

---

## 1. Global Coding Rules

### 1.1 Type System

* All functions **SHOULD** use type hints for parameters and return values.
* `Any` / `**kwargs` are allowed **only when justified**.
* Dynamic structures should prefer:

  * `TypedDict`
  * `pydantic.BaseModel`

Agents should favor **explicit schemas over ad-hoc dicts**.

---

### 1.2 SQLAlchemy Usage Policy

* Prefer:

  * `Table`, `Column`
  * `select / insert / update`
  * Explicit transactions
* ORM code must remain:

  * Explicit
  * Predictable
  * Easy to test

DB access should avoid hidden side effects and implicit state.

---

### 1.3 No Hidden Logic

* No logic in `__init__.py`
* No side effects at import time
* No implicit global state mutation

All behavior must be callable and testable.

---

## 2. Architecture Constraints

### 2.1 Directory Responsibilities

```
src/
├── api/routers/     # FastAPI endpoints only
├── core/            # Business logic (framework-agnostic)
├── services/        # Orchestration & workflows
├── infra/           # DB, logging, external clients
├── models/          # Pydantic schemas (and ORM models if needed)
├── config/          # Centralized configuration
tests/

Notes:
- core/pipeline: pipeline interfaces, types, validation, registry
- core/chunking: chunk policies for splitting arguments into chunks
- core/fetch: fetcher interfaces, retry policy, fetch errors
- core/clean: cleaner interfaces and validation helpers
- core/workflow: workflow orchestration and failover policies
- infra/http_client: HTTP client adapters (no direct requests in business logic)
- infra/fetcher: fetcher implementations
- infra/queue: task queue interfaces/implementations
- infra/worker_runtime: background worker runtime
- services/worker_handler: task execution handler for pipelines
- services/workflow_engine: pipeline-aware workflow orchestrator
- config/task_pipeline_mapping.py: static spec→pipeline mapping
```

---

### 2.2 Dependency Direction (Strict)

```
api → services → core
            ↘︎ infra
```

Lower layers must not import higher layers.

---

## 3. Configuration

* All configuration is centralized.
* Business logic must NOT read env vars directly.
* Adding config requires:

  * Clear name
  * Default value
  * Single source of truth

---

## 4. Logging

* Use `logging`, never `print`
No ad-hoc loggers.

---

## 5. Testing Contract

### 5.1 General

* All tests under `tests/`
* Files: `test_*.py`
* Functions: `test_*`

### 5.2 Unit Tests

* No real DB
* No network
* No shared external state

### 5.3 Integration Tests

* Real database
* External APIs mocked
* FastAPI tested via `TestClient`

---

## 6. Quality Gates (Mandatory)

Before any change is valid:

```bash
ruff check .
ruff format .
mypy src/
pytest
```

Agents must assume CI enforces this.

---

## 7. Code Style Expectations

* Small, composable functions
* Comments explain **why**, not **what**
* Public classes/functions should include concise docstrings (purpose, role, params)

If logic cannot be clearly explained, it is considered incorrect.

---

## 8. Uncertainty & Clarification Rule (Critical)

Before writing or modifying code, the agent MUST check for uncertainty.

Uncertainty includes:
- unclear or missing requirements
- multiple reasonable interpretations
- missing constraints or priorities
- changes with broad or irreversible impact

If ANY uncertainty exists:
- STOP implementation
- LIST the uncertainties
- ASK the user for clarification
- WAIT for confirmation

The agent MUST NOT make assumptions or proceed based on best guesses.

Default rule: **ASK FIRST. DO NOT ASSUME.**

---

## 9. Document Update Rules

This document MUST be updated when any of the following occur:

1. Architecture or core rules change
   - layering, boundaries, stability definitions

2. New modules are introduced
   - especially when stability level or responsibility differs

3. Runtime or execution environment changes
   - language version, framework, platform, deployment assumptions

4. Repeated user intent
   - the same instruction, constraint, or preference is mentioned **more than three times**
   - statements must be explicit and consistent in meaning

For case (4):
- Treat it as a candidate project rule
- Propose adding it to this document
- Show the exact suggested wording

Rule of thumb:
- 1–2 times: situational
- ≥3 times: project-level intent

Do NOT infer rules from vague, implicit, or conflicting statements.

---
