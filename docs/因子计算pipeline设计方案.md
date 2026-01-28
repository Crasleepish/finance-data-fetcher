# 因子计算 pipeline 设计方案（market_factors_pipeline）

## 1. 目标与产出

**目标**：基于历史行情与基本面数据构建组合，并计算日度因子收益（MKT/SMB/HML/QMJ），默认写入 `market_factors` 表。  
**产出表**：`market_factors(date, MKT, SMB, HML, QMJ)`，均为**小数**（如 1% 记为 0.01）。

## 2. 上游数据前提

### 2.1 数据表依赖

以下表数据被视为可信输入：

- `stock_hist_unadj`：日线行情与估值/成交额/市值数据（含 `close`, `mkt_cap`, `amount`）。
- `adj_factor`：复权因子，用于构造“复权价格”。
- `fundamental_data`：财务字段（`total_equity`, `operating_profit_ttm`, `total_assets`,
  `total_liabilities`, `net_profit`, `net_cash_from_operating`）。
- `stock_info`：行业与上市信息（`industry`, `listing_date`, `exchange`）。
- `index_hist`：基准指数行情（`index_code = 000985.CSI` 的 `change_percent`）。
- `trade_calendar`：交易日历，用于调仓日与回测区间。

### 2.2 数据窗口

对输入区间 `[start_date, end_date]`：

- 复权价格、成交额、市值：取 **前一交易日** 到 `end_date`。
  - `prev_start_date = prev_trade_day(start_date)`。
- 基本面数据：
  - `history` 模式：`[start_date - 500d, end_date - 120d]`  
    目的：避免“未来财报”泄露。
  - `realtime` 模式：`[start_date - 500d, end_date]`  
    目的：允许最新季度数据参与实时因子计算。

## 3. 数据清洗与标准化

### 3.1 行情与复权价格

使用 `stock_hist_unadj` 的 `close` 作为未复权价格，结合 `adj_factor`：

```
adjusted_price = close * (adj_factor / latest_adj_factor_per_stock)
```

其中 `latest_adj_factor_per_stock` 为该股票在 end_date 的最新复权因子。

### 3.2 基本面数据

在选择器阶段以“最近可用财报”为准：

- 对每只股票，在 `asof_date` 前 1 年内寻找最新 `report_date`。
- 若相关字段缺失则该股票不可参与质量/价值打分。

## 4. 组合构建与调仓规则

### 4.1 调仓日

使用 `RebalanceDateGenerator`：

- 频率：每季度末（3/6/9/12 月），`anchor = end`。
- 在回测中 **显式加入 `prev_start_date`** 作为首个调仓日，
  若历史权重缺失，则该日收益可能为 0（已有日志提示）。

### 4.2 股票池筛选链

选择器链路（顺序执行，前一层过滤结果作为下一层候选）：

1. **BasicSelector**  
   - 基于 `stock_info` + 价格数据，剔除基础不可用股票。
2. **AmountSelector**  
   - 剔除成交额最低 1% 的股票（`amount`）。
3. **MktCapPercentileSelector**  
   - 小盘：市值分位区间 `[0, 0.5]`
   - 大盘：市值分位区间 `[0.5, 1.0]`

### 4.3 因子分组

组合共 12 个：`BM/QMJ × 小盘/大盘 × 低/中/高`。

#### BM 分组（价值因子）

- 使用 `BMScoreSelector`：以 **B/M** 估值排序（来自基本面 + 市值）。
- 低/中/高分位：`[0.0, 0.3)`, `[0.3, 0.7)`, `[0.7, 1.0]`。

#### QMJ 分组（质量因子）

使用 `QualityScoreSelector`，质量分数由行业 z-score 计算：

```
profit = operating_profit_ttm / total_equity
cfq    = net_cash_from_operating / net_profit
lev    = total_liabilities / total_assets

score = zscore(profit) + zscore(cfq) - zscore(lev)
```

- 行业内 z-score 采用 **去除极值**（min/max）后的均值/方差。
- 若行业样本过小，退化为全样本统计。
- 低/中/高分位与 BM 相同。

### 4.4 权重分配

`MktCapWeightAllocator` 按市值加权（权重归一化）。

## 5. 回测与组合日收益

使用 `VectorBT` 进行回测：

```
init_cash = 100_000_000
buy_fee = 0
sell_fee = 0
slippage = 0
cash_sharing = True
```

回测产出：

- `*_weights.csv`：调仓日权重矩阵
- `*_daily_returns.csv`：日收益序列（字段 `date, value`）

> **因子计算逻辑使用 `*_daily_returns.csv`**，而非用固定权重手算单日收益。

## 6. 因子定义与计算

记组合日收益：

```
bm_SL, bm_SM, bm_SH, bm_BL, bm_BM, bm_BH
qmj_SL, qmj_SM, qmj_SH, qmj_BL, qmj_BM, qmj_BH
```

### 6.1 SMB（规模因子）

```
SMB_bm  = mean(bm_SL, bm_SM, bm_SH) - mean(bm_BL, bm_BM, bm_BH)
SMB_qmj = mean(qmj_SL, qmj_SM, qmj_SH) - mean(qmj_BL, qmj_BM, qmj_BH)
SMB     = (SMB_bm + SMB_qmj) / 2
```

### 6.2 HML（价值因子）

```
HML = mean(bm_SH, bm_BH) - mean(bm_SL, bm_BL)
```

### 6.3 QMJ（质量因子）

```
QMJ = mean(qmj_SH, qmj_BH) - mean(qmj_SL, qmj_BL)
```

### 6.4 MKT（市场因子）

来自 `index_hist`：

```
MKT = change_percent(000985.CSI) / 100
```

若缺失则填 0。

## 7. 入库规则

- 输出记录按日期写入 `market_factors`。
- 数值为小数形式（非百分数）。
- 批量写入通过 Repository，幂等由主键 `(date)` 保证。
- **dry-run 模式**：当 `arguments.params.dry_run = true` 时，仍会生成 `bt_result` 中间产物，
  但**不**写入 `market_factors` 表，任务成功结束。

## 8. 关键一致性检查

为保证结果可信：

1. `bt_result/*_daily_returns.csv` 与 `market_factors` 计算一致。
2. 组合权重符合直觉：
   - 小盘组合市值显著低于大盘组合。
   - 质量低组基本面显著弱于质量高组。
3. 若使用固定权重单日手算，与因子结果差异是**正常**的
   （因回测收益包含权重漂移与真实交易逻辑）。

## 9. 运行模式差异

| 模式 | 基本面窗口 | 目的 |
| --- | --- | --- |
| history | end_date - 120d | 避免财报未来数据泄露 |
| realtime | end_date | 使用最新季度数据 |

补充：`dry_run` 与 `mode` 独立，`dry_run=true` 不影响组合构建与回测逻辑，仅跳过入库。

## 10. 依赖文件与产物

- 输入：
  - DB 表：`stock_hist_unadj`, `adj_factor`, `fundamental_data`, `stock_info`, `index_hist`, `trade_calendar`
- 中间产物：
  - `bt_result/price.csv`
  - `bt_result/mkt_cap.csv`
  - `bt_result/amount.csv`
  - `bt_result/fundamentals.csv`
  - `bt_result/*_weights.csv`
  - `bt_result/*_daily_returns.csv`
- 输出：
  - `market_factors`
