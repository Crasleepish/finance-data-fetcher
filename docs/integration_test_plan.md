# Integration Test Plan

## Context
- Change summary: add get_stock_info pipeline (Tushare fetcher + cleaner), wire into API/workflow, route pipeline to stock_info repo with upsert, expand task spec/mapping, add real-DB integration tests.
- Architecture references (docs/*.dot): docs/arc_diag.dot (API → queue → worker → workflow → pipeline → repo → DB).
- Assumptions: real Postgres reachable per config/app.yaml; Tushare token valid; stock_info table exists.

## Impact Map
- APIs: POST /tasks/start accepts spec get_stock_info; GET /tasks/{id}, /tasks/running for status/progress.
- Events/Queues: in-memory queue + worker runtime handles async execution.
- DB schema/data contracts: stock_info table (upsert by stock_code), task_table/task_payload used for status tracking.
- External dependencies: Tushare SDK/API (stock_basic) with retry policy.
- Config/runtime flags: config/task_pipeline_mapping.py and config.tushare.token.

## Test Matrix

### P0 (User-Facing Critical Path)
- Case ID: P0-GET-STOCK-INFO-E2E
  - Entry point: POST /tasks/start
  - Steps: start get_stock_info with params {exchange:"", list_statuses:["L","D","P"]} → poll /tasks/{id} until SUCCEEDED
  - Assertions (API/state/DB): task transitions PENDING/RUNNING → SUCCEEDED, progress reaches 100; stock_info contains rows for requested list_statuses
  - Data setup: none beyond live Tushare + existing stock_info table
  - Cleanup: none (explicitly retain data)

### P1 (Core Workflow)
- Case ID: P1-PIPELINE-CSV-E2E
  - Entry point: POST /tasks/start
  - Steps: start csv pipeline, ensure worker executes fetch/clean/persist with retry
  - Assertions (API/state/DB): status progresses to SUCCEEDED; test_messages inserts visible; idempotent start dedupes active run; rerun creates new task_id
  - Data setup: create integration_messages.csv in repo root
  - Cleanup: none (explicitly retain data)

### P2 (Edge Cases)
- Case ID: P2-UPsert-REPO-ROUTING
  - Entry point: POST /tasks/start
  - Steps: run get_stock_info and verify upsert path doesn’t create duplicates for stock_code
  - Assertions (API/state/DB): repeated run does not error; stock_info row count stable for unchanged inputs
  - Data setup: existing stock_info data
  - Cleanup: none

## Execution Prerequisites
- Environment: real Postgres (config/app.yaml), network to Tushare API, Tushare token.
- Data/mocks: no mocks; live Tushare; ensure stock_info table exists.
- Isolation strategy: use unique message IDs for test_messages; tolerate existing stock_info data.
- Cleanup policy: none.
