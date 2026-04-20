# finance-data-fetcher Wiki

## 项目简介

`finance-data-fetcher` 是一个面向金融数据采集、清洗与落库的可维护微服务。它把分散、一次性的抓取脚本抽象为可复用的 pipeline，并统一通过 HTTP 任务接口触发、后台异步执行和状态追踪。

## 项目价值

- 统一“抓取 → 清洗 → 入库”流程，降低数据采集任务的维护成本。
- 通过异步任务、幂等控制和显式事务，提高批量处理的稳定性与可追踪性。
- 通过清晰分层与配置化路由，让新增数据源、任务类型和 pipeline 更容易扩展。

## 核心特点

- **Pipeline 化编排**：以 `spec` / `task_type` 驱动 pipeline 选择，复用 fetcher、cleaner、repository 等组件。
- **异步任务执行**：FastAPI 接口负责接收任务，后台 worker 异步消费队列并执行工作流。
- **幂等与状态跟踪**：内置 active-run 去重、任务状态机、进度跟踪与任务查询接口。
- **金融场景支持**：内置交易日历归一化、分块抓取、失败重试等能力，适合历史与实时数据任务。
- **清晰架构边界**：遵循 `api → services → core → infra` 分层，降低耦合，便于测试与演进。
- **配置驱动扩展**：通过集中配置和 pipeline mapping 管理任务规格与实现映射，避免业务逻辑散落在入口层。

## 架构图

```mermaid
flowchart LR
    A[Client / Scheduler] --> B[FastAPI API<br/>/tasks/start /tasks/running /tasks/{id}]
    B --> C[TaskService + Idempotency Guard]
    C --> D[Task Queue]
    D --> E[Worker Runtime]
    E --> F[Workflow Engine]
    F --> G[Pipeline Selector / Registry]
    F --> H[TradingCalendarService]
    G --> I[Ingestion Pipeline]
    I --> J[ChunkPolicy]
    I --> K[Fetcher]
    I --> L[Cleaner]
    F --> M[Repository]
    M --> N[(Database)]
    F --> O[TaskStatusStore / Progress / Logs]
```

## 典型数据流

1. 调用 `/tasks/start` 提交任务。
2. 服务层校验并生成幂等键，避免重复运行。
3. 任务进入队列，由 worker 后台执行。
4. `WorkflowEngine` 根据 `spec` 选择 pipeline，并结合交易日历与分块策略抓取数据。
5. 数据经过清洗后，通过 repository 以显式事务写入数据库。
6. 任务状态、进度与错误信息持续写入状态存储，供查询接口和运维观察使用。
