# finance-data-fetcher

FastAPI microservice for fetch-clean-persist workflows.

## Run

```sh
git clone <this repo>
cd finance-data-fetcher
```

```sh
uv sync --dev
```

```sh
./run.sh --reload --host 0.0.0.0 --port 8000
```

## Example: start get_stock_info

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_stock_info",
    "source": "manual",
    "task_type": "stock_info",
    "arguments": {
      "params": {
        "exchange": "",
        "list_statuses": ["L", "D", "P"]
      }
    },
    "options": {}
  }'
```

## Example: start get_stock_hist_unadj

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_stock_hist_unadj",
    "source": "manual",
    "task_type": "stock_hist_unadj",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

## Example: start get_fundamental_data

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_fundamental_data",
    "source": "manual",
    "task_type": "fundamental_data",
    "arguments": {
      "params": {
        "start_period": "2023-12-31",
        "end_period": "2024-09-30",
        "overwrite": false
      }
    },
    "options": {}
  }'
```

## Example: start get_market_factors

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_market_factors",
    "source": "manual",
    "task_type": "market_factors",
    "arguments": {
      "params": {
        "start_date": "2023-01-02",
        "end_date": "2023-06-30",
        "mode": "history"
      }
    },
    "options": {}
  }'
```

## Example: start get_fund_info

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_fund_info",
    "source": "manual",
    "task_type": "fund_info",
    "arguments": {
      "params": {
        "market": "O",
        "status": "L"
      }
    },
    "options": {}
  }'
```

## Example: start get_adj_factor

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_adj_factor",
    "source": "manual",
    "task_type": "adj_factor",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

## Example: start get_index_info

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_index_info",
    "source": "manual",
    "task_type": "index_info",
    "arguments": {
      "params": {
        "markets": ["CSI", "SSE", "SZSE"],
        "csv_path": "extra/additional_index_info.csv"
      }
    },
    "options": {}
  }'
```

## Example: start get_index_hist_stock

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_index_hist_stock",
    "source": "manual",
    "task_type": "index_hist_stock",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

## Example: start get_index_hist_bond

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_index_hist_bond",
    "source": "manual",
    "task_type": "index_hist_bond",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

## Example: start get_index_hist_gold

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_index_hist_gold",
    "source": "manual",
    "task_type": "index_hist_gold",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

## Example: start get_gold_cftc_report

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_gold_cftc_report",
    "source": "manual",
    "task_type": "gold_cftc_report",
    "arguments": {
      "params": {
        "as_of_date": "2026-01-02"
      }
    },
    "options": {}
  }'
```

## Example: start get_gold_future_curve

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_gold_future_curve",
    "source": "manual",
    "task_type": "gold_future_curve",
    "arguments": {
      "params": {}
    },
    "options": {}
  }'
```
