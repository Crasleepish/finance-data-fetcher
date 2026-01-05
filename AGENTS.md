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
extra/             # Supplemental CSV inputs (manual overrides)

Notes:
- core/pipeline: pipeline interfaces, types, validation, registry
- core/chunking: chunk policies for splitting arguments into chunks
- core/fetch: fetcher interfaces, retry policy, fetch errors
- core/clean: cleaner interfaces and validation helpers
- core/workflow: workflow orchestration and failover policies
- infra/http_client: HTTP client adapters (no direct requests in business logic)
- infra/fetcher: fetcher implementations
- infra/fetcher/csv_fetcher: CSV-backed fetcher for tests/examples
- infra/fetcher/tushare_stock_basic_fetcher: Tushare stock_basic fetcher
- infra/fetcher/tushare_stock_hist_fetcher: Tushare daily/daily_basic/stock_st/suspend fetcher
- infra/fetcher/tushare_adj_factor_fetcher: Tushare adj_factor fetcher
- infra/fetcher/tushare_index_basic_fetcher: Tushare index_basic fetcher
- infra/fetcher/tushare_index_daily_fetcher: Tushare index_daily fetcher
- infra/fetcher/tushare_sge_daily_fetcher: Tushare sge_daily fetcher
- infra/fetcher/akshare_index_hist_fetcher: Akshare csindex fetcher
- infra/fetcher/tushare_fund_basic_fetcher: Tushare fund_basic fetcher
- infra/fetcher/tushare_fund_nav_fetcher: Tushare fund_nav fetcher
- infra/fetcher/tushare_fund_daily_fetcher: Tushare fund_daily fetcher
- infra/fetcher/tushare_fundamental_fetcher: Tushare vip fundamental fetcher
- infra/fetcher/tushare_fundamental_single_fetcher: Tushare non-vip fundamental fetcher
- infra/tushare/client: Tushare client wrapper for SDK access
- infra/factor_data_fetcher: Factor backtest data fetchers (stock/index/calendar)
- infra/gold_derivatives_fetcher: Fetcher for gold CFTC reports and futures curve (raw batches)
- infra/fund_beta_data_fetcher: Data access for fund beta estimation
- core/clean/csv_cleaner: CSV cleaner for test_messages
- core/clean/stock_hist_unadj_cleaner: Cleaner for stock_hist_unadj
- core/clean/adj_factor_cleaner: Cleaner for adj_factor
- core/clean/index_info_cleaner: Cleaner for index_info
- core/clean/index_hist_stock_cleaner: Cleaner for stock index history
- core/clean/index_hist_bond_cleaner: Cleaner for bond index history
- core/clean/index_hist_gold_cleaner: Cleaner for gold index history
- core/clean/fund_info_cleaner: Cleaner for fund_info
- core/clean/fund_hist_cleaner: Cleaner for fund_hist
- core/clean/etf_info_cleaner: Cleaner for etf_info
- core/clean/etf_hist_cleaner: Cleaner for etf_hist
- core/clean/market_factors_cleaner: Pass-through cleaner for market_factors
- core/clean/fund_beta_cleaner: Cleaner for fund_beta
- core/indexing/index_codes: index code parsing and API mapping helpers
- core/clean/fundamental_data_cleaner: Cleaner for fundamental_data
- core/beta/kalman_filter: Kalman filter with ECM support
- core/beta/q_r_estimator: Q/R estimator for beta regression
- core/beta/covariance: Covariance pack/unpack helpers
- core/clean/gold_cftc_report_cleaner: Cleaner for gold_cftc_report
- core/clean/gold_future_curve_cleaner: Cleaner for gold_future_curve
- infra/queue: task queue interfaces/implementations
- infra/worker_runtime: background worker runtime
- services/worker_handler: task execution handler for pipelines
- services/workflow_engine: pipeline-aware workflow orchestrator
- services/pipelines: pipeline implementations (stock_info, etc.)
- infra/index_catalog: index_info lookup helpers
- infra/fund_catalog: fund_info lookup helpers
- infra/etf_catalog: etf_info lookup helpers
- services/pipelines/fund_hist_index_pipeline: index fund NAV pipeline
- services/pipelines/fund_hist_money_pipeline: money fund NAV pipeline
- services/pipelines/etf_info_pipeline: ETF fund_basic pipeline
- services/pipelines/etf_hist_pipeline: ETF daily history pipeline
- services/pipelines/market_factors_pipeline: Factor computation pipeline to persist market_factors
- services/pipelines/gold_cftc_report_pipeline: Gold CFTC report pipeline
- services/pipelines/gold_future_curve_pipeline: Gold futures curve pipeline
- services/pipelines/fund_beta_pipeline: Fund beta pipeline
- services/portfolio_driver: Portfolio construction/backtest orchestration
- services/factor_fetcher: Factor backtest entrypoint returning RawBatch
- services/fund_beta_estimator: Fund beta estimation logic
- core/backtest/backtest_engine: VectorBT-based backtest engine
- core/backtest/stock_selector: Factor portfolio selectors
- core/backtest/weight_allocator: Portfolio weight allocation strategies
- core/backtest/rebalance_date_generator: Rebalance date generation helpers
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
* Logs must be traceable, debuggable, not excessive, and must not leak sensitive data.

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
