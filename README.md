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
