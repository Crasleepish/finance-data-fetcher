# Test Report

## Scope
- Integration tests (real database + FastAPI TestClient + in-process worker)

## Commands Executed
- `uv run pytest tests/test_real_db_full_chain.py tests/test_real_db_stock_info_pipeline.py`

## Expected vs Actual Results
- Expect: CSV pipeline and get_stock_info pipeline complete asynchronously with SUCCEEDED + progress 100; persistence succeeds.
- Actual: both tests passed. Warnings: testcontainers deprecation warnings.

## Final Status
✅ PASS
