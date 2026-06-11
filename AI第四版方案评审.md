内部 HTTP 通知投递服务 — 技术设计方案

一、背景与目标

企业内部多个业务系统在关键事件发生时，需要调用外部供应商 HTTPS API 进行通知。不同供应商 API 地址、Header、Body
格式各不相同。业务系统不关心返回值，只需确保通知被可靠送达。

核心目标： 设计一个中继服务，接收业务系统的通知请求，尽可能可靠地投递到目标地址（at-least-once）。

需求边界

- 上游请求协议统一，不需要适配不同结构
- 下游所需 body 数据均可从上游请求中获取
- 链路中必须携带唯一幂等 ID
- 下游 HTTP 返回 200 即视为成功
- 不依赖上游请求时序，不保证下游调用顺序
- 不考虑黑产风控、上游重试策略、数据库/中间件彻底宕机、性能瓶颈
- 允许偶尔失败（最终通过重试兜底）

 ---
二、整体架构

┌─────────────────────────────────────────────────────────────────────────────────┐
│                        通知中继服务 (Notification Relay)                          │
│                                                                                 │
│                                                                                 │
│  ┌──────────────┐      ┌───────────────────┐      ┌────────────────────────┐   │
│  │  接入层 API  │─────▶│  asyncio 调度器    │─────▶│  投递协程池            │   │
│  │  (FastAPI)   │      │  (call_later)      │      │  (httpx + Semaphore)   │   │
│  └──────┬───────┘      └───────────────────┘      └───────────┬────────────┘   │
│         │                       ▲                              │                │
│         │ 持久化                │ 失败重入队(带延迟)            │ HTTP 请求      │
│         ▼                       │                              ▼                │
│  ┌──────────────┐      ┌───────┴───────────┐      ┌────────────────────────┐   │
│  │ SQLite/Prisma │      │  重试调度器        │      │  外部供应商 HTTPS API  │   │
│  │  (持久化层)  │◀─────│  (Prisma 回写)     │      └────────────────────────┘   │
│  └──────────────┘      └───────────────────┘                                   │
│         ▲                                                                       │
│         │ 启动时恢复未完成任务                                                    │
│         │                                                                       │
│  ┌──────┴───────┐                                                               │
│  │  启动恢复器  │                                                               │
│  │  (Recovery)  │                                                               │
│  └──────────────┘                                                               │
└─────────────────────────────────────────────────────────────────────────────────┘

核心设计思路：内存驱动 + DB 持久化

关键原则：不轮询数据库。 正常运行时所有调度都在内存中完成，SQLite 只做持久化保障。

数据流：

1. 请求接入：业务系统 POST 通知 → 接入层校验 + 写入 SQLite（status=PENDING）→ 立即投入内存延迟队列 → 返回 202
2. 首次投递：Worker 从内存队列取出任务 → 发送 HTTP 请求 → 成功则回写 SQLite（status=DELIVERED）
3. 失败重试：投递失败 → 计算退避延迟 → 任务重新放入内存延迟队列（带延迟时间）→ 同时回写 SQLite（status=RETRYABLE, next_attempt_at）
4. 重试耗尽：达到最大重试次数 → 回写 SQLite（status=FAILED）
5. 启动恢复：服务重启时，从 SQLite 加载所有未完成任务（PENDING/RETRYABLE 且 next_attempt_at <= now）到内存队列

 ---
三、为什么不轮询 DB

┌─────────────────────┬────────────────────┬──────────────────┬──────────────────────┐
│        方案         │        延迟        │     DB 压力      │        扩展性        │
├─────────────────────┼────────────────────┼──────────────────┼──────────────────────┤
│ DB 轮询（每 500ms） │ 最大 500ms         │ 高（持续查询）   │ 多实例需 SKIP LOCKED │
├─────────────────────┼────────────────────┼──────────────────┼──────────────────────┤
│ 内存延迟队列        │ 接近 0（直接入队） │ 低（仅读写状态） │ 单实例内完成调度     │
└─────────────────────┴────────────────────┴──────────────────┴──────────────────────┘

内存队列方案下：
- 正常流量：任务从 API 接收直接进入内存队列，Worker 立即消费，全程不查 DB
- 失败重试：任务带延迟时间重入内存队列，到期后 Worker 自动取出，不查 DB
- 唯一需要读 DB 的场景：服务启动时恢复未完成任务

持久化保障： 每个任务在进入内存队列之前，一定先写入 SQLite。即使进程崩溃，重启后可从 DB 恢复所有未完成任务，不丢消息。

 ---
四、核心组件设计

4.1 内存延迟队列

使用 Python asyncio 调度机制：

- 首次提交的任务：asyncio.create_task() 立即执行投递协程
- 重试任务：asyncio.get_event_loop().call_later(delay, callback) 延迟后重新投递
- 通过 asyncio 事件循环天然管理所有任务的并发和延迟调度

好处：
- 无轮询，事件驱动（asyncio 原生）
- 重试延迟精确（event loop timer 精度）
- 单线程协程模型，无竞争、无锁
- 协程并发数可通过 asyncio.Semaphore 控制

4.2 投递 Worker

- 每个任务是一个独立的 async 协程
- 使用 httpx.AsyncClient 发送 HTTP 请求到 target_url
- 判定逻辑：
    - HTTP 200 → DELIVERED
    - 非 200 或异常 → call_later 延迟后重试
    - 达到最大重试次数 → FAILED
- 使用 asyncio.Semaphore 限制最大并发投递数

4.3 启动恢复器（Recovery）

服务启动时执行一次：

SELECT * FROM notification_task
WHERE status IN ('PENDING', 'RETRYABLE')

将这些任务加载到内存延迟队列：
- 如果 next_attempt_at <= NOW()：delay = 0，立即投递
- 如果 next_attempt_at > NOW()：delay = next_attempt_at - NOW()

这是唯一一次读取 DB 作为调度用途。

4.4 部署模式

单实例部署，asyncio 事件循环天然无竞争，不考虑多实例扩展。

 ---
五、数据库设计（单表，Prisma Schema）

// prisma/schema.prisma

datasource db {
provider = "sqlite"
url      = "file:./dev.db"
}

generator client {
provider             = "prisma-client-py"
recursive_type_depth = 5
}

model NotificationTask {
id             Int      @id @default(autoincrement())
idempotencyKey String   @unique @map("idempotency_key")
targetUrl      String   @map("target_url")
headers        String   @default("{}") // JSON 字符串，下游所需 headers
body           String?                 // 请求体，从上游原样透传
status         String   @default("PENDING")
attemptCount   Int      @default(0) @map("attempt_count")
maxAttempts    Int      @default(5) @map("max_attempts")
nextAttemptAt  DateTime @default(now()) @map("next_attempt_at")
lastStatusCode Int?     @map("last_status_code")
lastError      String?  @map("last_error")
createdAt      DateTime @default(now()) @map("created_at")
updatedAt      DateTime @updatedAt @map("updated_at")

@@index([status, nextAttemptAt], name: "idx_recovery")
@@map("notification_task")
}

字段精简说明：
- 去掉 sourceSystem、eventType：不做分类统计
- 去掉 httpMethod：约定全部为 POST
- 去掉 contentType、timeoutMs：使用全局默认值
- 保留核心字段：幂等键、目标地址、请求头、请求体、状态机

状态机：

PENDING ──首次投递──▶ DELIVERED（终态）
│
│ 失败且未达上限
▼
RETRYABLE ──重试成功──▶ DELIVERED（终态）
│
│ 达到 max_attempts
▼
FAILED（终态）

说明：
- Prisma 自动管理 @unique 和 @@index，无需手写 SQL
- 通过 prisma migrate dev 自动生成迁移和 SQLite 文件
- 生产如需切换数据库，只改 provider（如 mysql、postgresql）

 ---
六、重试策略

指数退避 + 抖动

delay = min(initialDelay × 2^(attempt-1) + jitter, maxDelay)

默认值：
initialDelay = 1s
multiplier = 2
jitter = random(0, delay × 0.2)
maxDelay = 1 小时
maxAttempts = 5

失败判定（极简）

┌────────────────────────────┬──────────────────┐
│            结果            │       动作       │
├────────────────────────────┼──────────────────┤
│ HTTP 200                   │ status=DELIVERED │
├────────────────────────────┼──────────────────┤
│ 非 200（含网络异常、超时） │ 重入队列 + 退避  │
├────────────────────────────┼──────────────────┤
│ 达到 max_attempts          │ status=FAILED    │
└────────────────────────────┴──────────────────┘

▎ 需求约定：下游返回 200 即成功，其他一律重试。

 ---
七、可靠性保障

7.1 不丢消息

- 先持久化，后入队：任务写入 SQLite 成功后才放入内存队列。即使入队前崩溃，重启恢复可兜底
- 先投递，后更新状态：投递成功后才标记 DELIVERED。即使标记前崩溃，重启后会重新投递（at-least-once）

7.2 进程崩溃恢复

- 启动恢复器加载所有 PENDING/RETRYABLE 任务到内存队列
- 已经 DELIVERED/FAILED 的不会被重新投递

7.3 幂等保证

- idempotency_key 全局唯一约束
- 重复提交直接返回已有任务状态，不会产生重复投递
- 调用方有责任生成全局唯一的幂等键（如 {event_type}:{business_id}:{timestamp}）

7.4 任务超时保护

- 如果某个任务长时间处于 PENDING 状态（内存队列丢失但 DB 已写入），启动恢复机制兜底
- 可选：增加定时检查（如每 10 分钟检查一次是否有"滞留"任务），作为防御性兜底，而非主要调度手段

 ---
八、技术选型

┌─────────────┬────────────────────────────────────────────────────────┬───────────────────────────────────────┐
│    组件     │                          选型                          │                 说明                  │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ 语言        │ Python 3.11+                                           │ asyncio 原生异步、跨平台              │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ Web 框架    │ FastAPI                                                │ 高性能异步框架、自动生成 OpenAPI 文档 │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ ORM         │ Prisma Client Python                                   │ 类型安全、自动迁移、跨数据库支持      │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ 数据库      │ SQLite（零依赖，单文件；生产可切换 SQLite/PostgreSQL） │ 持久化层                              │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ HTTP 客户端 │ httpx（AsyncClient）                                   │ 异步、连接池、超时控制                │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ 任务调度    │ asyncio 事件循环（create_task / call_later）           │ 内存延迟调度                          │
├─────────────┼────────────────────────────────────────────────────────┼───────────────────────────────────────┤
│ 并发控制    │ asyncio.Semaphore                                      │ 限制最大并发投递数                    │
└─────────────┴────────────────────────────────────────────────────────┴───────────────────────────────────────┘

为什么选 Python + Prisma + SQLite

- 零依赖部署：SQLite 单文件数据库，无需安装数据库服务，克隆即可运行
- 跨平台：Python + Prisma + SQLite 在 macOS/Linux/Windows 上均可运行
- Prisma 优势：schema-first 设计、prisma migrate 自动管理表结构、生产环境可一行切换到 MySQL/PostgreSQL
- asyncio 优势：单线程协程模型，代码简洁，天然避免并发竞争
- 极简启动：pip install → prisma migrate dev → uvicorn app.main:app，三步启动

 ---
九、项目结构

notification-relay/
├── pyproject.toml              # 依赖管理
├── prisma/
│   └── schema.prisma           # Prisma 数据模型定义
├── app/
│   ├── main.py                 # FastAPI 应用入口 + 启动恢复
│   ├── api/
│   │   └── notifications.py    # REST 接口
│   ├── services/
│   │   ├── ingest.py           # 接收、校验、持久化、入队
│   │   ├── dispatcher.py       # 内存调度 + asyncio 管理
│   │   ├── delivery.py         # HTTP 投递 + 结果分类
│   │   └── retry.py            # 退避计算
│   └── models/
│       └── schemas.py          # Pydantic 请求/响应模型
└── tests/
└── ...

▎ SQLite 为单文件数据库（prisma/dev.db），无需安装任何数据库服务，prisma migrate dev 即自动创建。

 ---
十、实施步骤

1. 项目脚手架（pyproject.toml、FastAPI 入口）
2. Prisma schema 定义 + prisma migrate dev（自动生成 SQLite 文件）
3. 接入层 API（FastAPI 路由 + Pydantic 校验 + Prisma 持久化 + 入队）
4. asyncio 调度器 + httpx 投递逻辑 + 退避计算 + 状态回写
5. 启动恢复器（FastAPI lifespan event 中加载未完成任务）
6. 测试（pytest + pytest-asyncio，模拟成功/失败/重试/恢复场景）