# Task API：task_type 参数说明

本文档面向前端对接，说明 `/tasks/start` 中 `task_type` 对应的 `arguments.params` 格式与约束，并给出调用示例（示例参考 README）。

## 请求结构（通用）

`POST /tasks/start`

```json
{
  "spec": "get_stock_info",
  "pipeline_id": null,
  "source": "manual",
  "task_type": "stock_info",
  "arguments": {
    "params": {}
  },
  "options": {}
}
```

### 字段说明

- `spec`：任务规格（`TaskSpec` 枚举），决定后端选择的 pipeline。
- `pipeline_id`：可选。通常留空/`null`，由后端根据 `spec` 选择 pipeline；仅在需要强制指定 pipeline 时填写。
- `source`：调用来源标识（任意字符串）。
- `task_type`：前端侧的任务类型标识（字符串）。后端不校验该值，但会参与幂等与日志。
- `arguments`：任务参数对象；目前仅使用 `arguments.params`。
- `options`：扩展选项对象（当前未使用，但会参与幂等）。

### 参数通用校验

`arguments` 与 `options` 必须是 **JSON 可序列化且 JSON 可比较** 的对象：
- 允许：字符串、数字、布尔、`null`、数组、对象。
- 不允许：`NaN`、集合、函数、`datetime` 等非 JSON 类型。

### spec ↔ task_type 建议映射

后端选择 pipeline 由 `spec` 决定；`task_type` 建议与 pipeline 语义保持一致，以下为推荐组合：

| spec | task_type | pipeline candidates |
| --- | --- | --- |
| get_stock_info | stock_info | stock_info |
| get_stock_hist_unadj | stock_hist_unadj | stock_hist_unadj |
| get_stock_is_st_from_file | stock_is_st_from_file | stock_is_st_from_file |
| get_fundamental_data | fundamental_data | fundamental_data, fundamental_data_single |
| get_market_factors | market_factors | market_factors |
| get_fund_beta | fund_beta | fund_beta |
| get_rt_stock_hist_unadj | rt_stock_hist_unadj | rt_stock_hist_unadj_tushare, rt_stock_hist_unadj_akshare |
| get_rt_index_hist | rt_index_hist | rt_index_hist_xueqiu, rt_index_hist_akshare |
| get_rt_etf_hist | rt_etf_hist | rt_etf_hist_akshare, rt_etf_hist_xueqiu |
| get_rt_market_factors | rt_market_factors | rt_market_factors |
| get_fund_info | fund_info | fund_info |
| get_etf_info | etf_info | etf_info |
| get_etf_hist | etf_hist | etf_hist |
| get_fund_hist_index | fund_hist_index | fund_hist_index |
| get_fund_hist_money | fund_hist_money | fund_hist_money |
| get_adj_factor | adj_factor | adj_factor |
| get_index_info | index_info | index_info |
| get_index_hist_stock | index_hist_stock | index_hist_stock |
| get_index_hist_bond | index_hist_bond | index_hist_bond |
| get_index_hist_gold | index_hist_gold | index_hist_gold |
| get_index_hist_global | index_hist_global | index_hist_global |
| get_internal_index | internal_index | internal_index |
| get_market_daily_info | market_daily_info | market_daily_info |
| get_gold_cftc_report | gold_cftc_report | gold_cftc_report |
| get_gold_future_curve | gold_future_curve | gold_future_curve |

> 说明：`pipeline candidates` 为后端可能的 pipeline 列表（按优先级顺序）。

---

## task_type 参数详解与示例

### stock_info

- **spec**：`get_stock_info`
- **arguments.params**：
  - `exchange`：`string`，可选，默认 `""`。
  - `list_statuses`：`list[string]` 或 `"L,D,P"` 字符串，可选，默认 `["L","D","P"]`。
  - `fields`：`string`，可选（Tushare 字段列表）。

**示例**

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

---

### stock_hist_unadj

- **spec**：`get_stock_hist_unadj`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。

**示例**

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

---

### stock_is_st_from_file

- **spec**：`get_stock_is_st_from_file`
- **arguments.params**（JSON 提交模式）：
  - `zip_path`：`string`，必填，zip 文件路径。
- **说明**：该任务只做全量处理；只更新 `stock_hist_unadj` 中已存在的 `(stock_code, date)` 行，不会插入新行。`is_st` 写入为 `int4`：`1 = ST`，`0 = 非 ST`，`NULL = 尚未回填`。

**JSON 示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_stock_is_st_from_file",
    "source": "manual",
    "task_type": "stock_is_st_from_file",
    "arguments": {
      "params": {
        "zip_path": "./extra/A_stock_daily_unadj.zip"
      }
    },
    "options": {}
  }'
```

**大文件上传示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/upload/stock-is-st-file" \
  -F "source=manual" \
  -F "file=@./extra/A_stock_daily_unadj.zip;type=application/zip"
```

> 说明：二进制大文件请使用 multipart 上传接口，服务端会流式落盘后再创建任务；若前面有反向代理，需要同步放开请求体大小限制。

---

### fundamental_data

- **spec**：`get_fundamental_data`
- **arguments.params**：
  - `start_period`：`string`，必填，季度末日期（`YYYY-MM-DD`，只能是 `03-31/06-30/09-30/12-31`）。
  - `end_period`：`string`，必填，季度末日期（同上）。
  - `overwrite`：`boolean`，可选，默认 `false`。
- **约束**：`start_period` 必须 `<= end_period`。

**示例**

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

---

### market_factors

- **spec**：`get_market_factors`
- **arguments.params**：
  - `start_date`：`string`，必填，建议 `YYYY-MM-DD`。
  - `end_date`：`string`，必填，建议 `YYYY-MM-DD`。
  - `mode`：`string`，可选，默认 `"history"`；当为 `"realtime"` 时会关闭回看窗口。
  - `dry_run`：`boolean`，可选，默认 `false`；为 `true` 时计算但不写入。

**示例（常规）**

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
        "mode": "history",
        "dry_run": false
      }
    },
    "options": {}
  }'
```

**示例（dry-run）**

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
        "mode": "history",
        "dry_run": true
      }
    },
    "options": {}
  }'
```

---

### fund_beta

- **spec**：`get_fund_beta`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `mode`：`string`，可选，默认 `"realtime"`，可选值 `"historical" | "realtime"`。
  - `fund_codes`：`list[string]`，可选；提供时必须为非空字符串列表。
- **依赖**：需要 `fund_hist` 与 `market_factors` 数据；仅计算满足条件的基金。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_fund_beta",
    "source": "manual",
    "task_type": "fund_beta",
    "arguments": {
      "params": {
        "start_date": "2025-10-13",
        "end_date": "2025-10-15",
        "mode": "historical",
        "fund_codes": ["019919.OF", "000001.OF"]
      }
    },
    "options": {}
  }'
```

---

### rt_stock_hist_unadj

- **spec**：`get_rt_stock_hist_unadj`
- **arguments.params**：无（可省略或传 `{}`）。
- **说明**：受实时抓取间隔限制，短时间内可能返回空任务。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_rt_stock_hist_unadj",
    "source": "manual",
    "task_type": "rt_stock_hist_unadj",
    "arguments": {
      "params": {}
    },
    "options": {}
  }'
```

---

### rt_index_hist

- **spec**：`get_rt_index_hist`
- **arguments.params**：无（可省略或传 `{}`）。
- **依赖**：配置的指数代码需存在于 `index_info` 表中。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_rt_index_hist",
    "source": "manual",
    "task_type": "rt_index_hist",
    "arguments": {
      "params": {}
    },
    "options": {}
  }'
```

---

### rt_etf_hist

- **spec**：`get_rt_etf_hist`
- **arguments.params**：无（可省略或传 `{}`）。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_rt_etf_hist",
    "source": "manual",
    "task_type": "rt_etf_hist",
    "arguments": {
      "params": {}
    },
    "options": {}
  }'
```

---

### rt_market_factors

- **spec**：`get_rt_market_factors`
- **arguments.params**：无（可省略或传 `{}`）。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_rt_market_factors",
    "source": "manual",
    "task_type": "rt_market_factors",
    "arguments": {
      "params": {}
    },
    "options": {}
  }'
```

---

### fund_info

- **spec**：`get_fund_info`
- **arguments.params**：
  - `market`：`string`，可选，默认 `"O"`。
  - `status`：`string`，可选，默认 `"L"`。
  - `fields`：`string`，可选（Tushare 字段列表）。

**示例**

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

---

### etf_info

- **spec**：`get_etf_info`
- **arguments.params**：
  - `market`：`string`，可选，默认 `"E"`。
  - `status`：`string`，可选，默认 `"L"`。
  - `fields`：`string`，可选（Tushare 字段列表）。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_etf_info",
    "source": "manual",
    "task_type": "etf_info",
    "arguments": {
      "params": {
        "market": "E",
        "status": "L"
      }
    },
    "options": {}
  }'
```

---

### etf_hist

- **spec**：`get_etf_hist`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：需要 `etf_info` 表中存在 ETF 代码。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_etf_hist",
    "source": "manual",
    "task_type": "etf_hist",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

---

### fund_hist_index

- **spec**：`get_fund_hist_index`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：需要 `fund_info` 表中存在指数/商品型基金，否则会报错。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_fund_hist_index",
    "source": "manual",
    "task_type": "fund_hist_index",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

---

### fund_hist_money

- **spec**：`get_fund_hist_money`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：基金代码来自配置 `data.fund.money`；若配置为空则不执行；若基金类型非“货币型”则报错。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_fund_hist_money",
    "source": "manual",
    "task_type": "fund_hist_money",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

---

### adj_factor

- **spec**：`get_adj_factor`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `fields`：`string`，可选（Tushare 字段列表）。

**示例**

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

---

### index_info

- **spec**：`get_index_info`
- **arguments.params**：
  - `markets`：`list[string]` 或 `"CSI,SSE,SZSE"` 字符串，可选，默认 `["CSI","SSE","SZSE"]`。
  - `csv_path`：`string`，可选，默认 `"extra/additional_index_info.csv"`。CSV 每行需 3 列：`index_code,index_name,market`。
  - `fields`：`string`，可选（Tushare 字段列表）。

**示例**

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

---

### index_hist_stock

- **spec**：`get_index_hist_stock`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：指数代码来自配置，且必须存在于 `index_info` 表中。

**示例**

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

---

### index_hist_bond

- **spec**：`get_index_hist_bond`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：指数代码来自配置，且必须存在于 `index_info` 表中。

**示例**

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

---

### index_hist_gold

- **spec**：`get_index_hist_gold`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：指数代码来自配置，且必须存在于 `index_info` 表中。

**示例**

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

---

### index_hist_global

- **spec**：`get_index_hist_global`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：指数代码来自配置，且必须存在于 `index_info` 表中。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_index_hist_global",
    "source": "manual",
    "task_type": "index_hist_global",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

---

### internal_index

- **spec**：`get_internal_index`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **依赖**：需要 `./bt_result` 下的因子与权重 CSV 文件存在（内部研究指数产出）。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_internal_index",
    "source": "manual",
    "task_type": "internal_index",
    "arguments": {
      "params": {
        "start_date": "2006-04-03",
        "end_date": "2006-04-30"
      }
    },
    "options": {}
  }'
```

---

### market_daily_info

- **spec**：`get_market_daily_info`
- **arguments.params**：
  - `start_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
  - `end_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。
- **说明**：抓取 Tushare 市场交易统计每日数据，双数据源：
  - 沪市（SH）：`pro.daily_info`（doc_id=215），请求带 `exchange='SH'` 过滤，并对响应行做防御性过滤（仅保留 SH 行）；
  - 深市（SZ）：`pro.sz_daily_info`（深圳市场每日交易概况），数据自 2008-01-02 起，无 fields/exchange 参数；原始金额/股本单位为元/股，入库时按 1e8 折算为亿元/亿股；`count` 映射为 `com_count`，`trans_count`/`pe`/`tr` 置 NULL，`exchange='SZ'`，`ts_name` 由分类映射生成（未知分类会报错）。
  - 深市分类映射完整覆盖 23 类（经 2008-01-02..2025-12-31 全量源扫描）：股票→SZ_MARKET、主板A股→SZ_A、主板B股→SZ_B、创业板A股→SZ_GEM_A、创业板→SZ_GEM、中小板→SZ_SME、基金→SZ_FUND、ETF→SZ_FUND_ETF、LOF→SZ_FUND_LOF、封闭式基金→SZ_FUND_CEF、分级基金→SZ_FUND_SF、基础设施基金→SZ_FUND_REIT、债券→SZ_BOND、债券现券→SZ_BOND_CN、债券回购→SZ_BOND_REP、企业债→SZ_BOND_ENT、公司债→SZ_BOND_COR、国债→SZ_BOND_GOV、可转换债券→SZ_BOND_CB、ABS→SZ_BOND_ABS、期权→SZ_OPTION、权证→SZ_WR、股票权证→SZ_WR。
  - 重复标签归一化：`权证` 与 `股票权证` 为同值重复标签（2008-01-02..2010-02-12），均映射到主键 `(trade_date, SZ_WR)`；清洗时对所有归一化后 `(trade_date, ts_code)` 相同的记录做精确去重——保留首条，完全相同则丢弃（保持输出顺序），内容冲突则报错（不做静默后写覆盖）。
  - 两个数据源各自按交易日独立分块（每块不超过 50 个交易日），沪市分块在前、深市分块在后；深市不会排定 2008-01-02 之前的任务块。
  - 表主键为 `(trade_date, ts_code)`，两源共用；源内 NULL/NaN 一律落为 NULL。

**示例**

```sh
curl -X POST "http://127.0.0.1:8000/tasks/start" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": "get_market_daily_info",
    "source": "manual",
    "task_type": "market_daily_info",
    "arguments": {
      "params": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05"
      }
    },
    "options": {}
  }'
```

---

### gold_cftc_report

- **spec**：`get_gold_cftc_report`
- **arguments.params**：
  - `as_of_date`：`string`，必填，格式 `YYYY-MM-DD` 或 `YYYYMMDD`。

**示例**

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

---

### gold_future_curve

- **spec**：`get_gold_future_curve`
- **arguments.params**：无（可省略或传 `{}`）。

**示例**

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
