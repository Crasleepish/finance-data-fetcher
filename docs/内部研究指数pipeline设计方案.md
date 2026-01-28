# 内部研究指数 pipeline 设计方案

## 1. 目标与产出

**目标**：在不重复构建组合的前提下，复用 `market_factors` 产生的组合权重/收益中间产物，计算 8 条内部研究指数并写入 `index_hist`，同时维护 `index_info`。  
**产出表**：  
- `index_hist(index_code, date, open, close, high, low, volume, amount, change_percent, change)`  
- `index_info(index_code, index_name, market, ...)`（新增/更新内部指数基础信息）  
**覆盖时间**：以 2006-03-31 为 1000 点基期，第一条数据为下一个交易日 2006-04-03。

## 2. 指数清单与口径

### 2.1 纯维度指数（4 条）

- **沪深全市场大盘**：`NYBIG.IN`  
  组合：`bm_BL + bm_BM + bm_BH`
- **沪深全市场小盘**：`NYSML.IN`  
  组合：`bm_SL + bm_SM + bm_SH`
- **沪深全市场价值**：`NYVAL.IN`  
  组合：`bm_SH + bm_BH`
- **沪深全市场成长**：`NYGRO.IN`  
  组合：`bm_SL + bm_BL`

### 2.2 交叉风格指数（4 条）

- **大盘价值**：`NYBV.IN`  
  组合：`bm_BH`
- **大盘成长**：`NYBG.IN`  
  组合：`bm_BL`
- **小盘价值**：`NYSV.IN`  
  组合：`bm_SH`
- **小盘成长**：`NYSG.IN`  
  组合：`bm_SL`

## 3. 依赖与缓存复用

### 3.1 依赖输入

- `bt_result/bm_??_daily_returns.csv`  
  6 个组合：`bm_SL, bm_SM, bm_SH, bm_BL, bm_BM, bm_BH`

### 3.2 复用策略

- **禁止重复组合构建**：直接复用 `market_factors` 生成的 `bt_result/*_daily_returns.csv`。
- 若 `bt_result` 不存在或缺失必要组合文件，应视为**前置条件未满足**并报错。

## 4. 指数计算规则

### 4.1 组合日收益构成

以 `bt_result/bm_??_daily_returns.csv` 中的 `value` 作为组合日收益（小数）。

### 4.2 指数日收益

对每条指数，按对应组合**等权平均**得到指数日收益：

```
r_index = mean(r_component_1, r_component_2, ...)
```

### 4.3 指数点位序列

基期：  
`2006-03-31 = 1000`  
第一条有效数据为下一交易日 `2006-04-03`。

计算公式：

```
index_t = index_{t-1} * (1 + r_index_t)
```

### 4.4 index_hist 字段映射

本指数为**日度点位序列**：

- `open` / `high` / `low`：与 `close` 相同  
  （保证字段完整，避免空值导致下游误判）
- `close`：指数点位
- `change`：`close - pre_close`
- `change_percent`：`change / pre_close * 100`
- `volume` / `amount`：来自 `stock_hist_unadj` 的成分股**日度汇总**

### 4.5 数值精度

- `open/high/low/close` **保留两位小数**后入库。

## 5. 入库规则

- 使用 `index_hist` 表存储。
- 主键：`(index_code, date)`。
- 每条指数独立写入，可分批插入。
- 同步写入 `index_info`：
  - `index_code`：上述 8 条代码
  - `index_name`：中文名（如“沪深全市场大盘”）
  - `market`：固定为 `IN`

## 6. 边界条件与异常处理

- 若缺失任意 `bm_??_daily_returns.csv`，直接抛错并记录原因。
- 若某日缺少组件收益，跳过该日（或记录为 NULL，按实现选择；推荐直接跳过）。
- 若基期交易日缺失，需从最早可用日开始，并将该日作为首日基点（但需日志注明）。

## 7. 预期验证

- 2006-04-03 作为首日，点位应基于 `2006-03-31` 基点计算。
- NYBIG vs NYSML 市值方向一致（对应大盘/小盘组合收益差异）。
- NYVAL vs NYGRO 价值/成长方向与 BM 分组逻辑一致。

## 8. 输出样例（示意）

```
index_code,date,open,close,high,low,volume,amount,change_percent,change
NYBIG.IN,2006-04-03,1000.00,1000.85,1000.85,1000.85,,,-0.00,0.85
```
