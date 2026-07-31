# LangSmith Platform 数据面对齐差距分析

> 分析对象：当前工作区（`feat/runtime-hardening`，aegra-api 0.16.0）
> 对齐目标：LangSmith Deployment / Agent Server 数据面 API
> 分析日期：2026-07-31

## 一、依据与方法

对齐的判定标准是「用官方 `langgraph-sdk` 的客户端能否正常工作」，所以以 SDK 的实际 HTTP 调用为准，而非官方文档描述。

| 来源 | 说明 | 权威性 |
|:--|:--|:--|
| `langgraph-sdk 0.4.2` 源码 | `.venv/Lib/site-packages/langgraph_sdk/_async/{assistants,runs,threads,cron,store}.py`，AST 提取每个方法的动词、路径、请求字段 | 最高 —— 这是客户端真实发出的请求 |
| —— 版本时效 | 0.4.2 发布于 2026-06-01，是分析当日 PyPI 上的**最新版本**（已核对）。仓库同时装有 `langgraph 1.2.6`，而 PyPI 最新为 1.2.10 —— 该库属运行时，不定义数据面 HTTP 契约，不影响本文结论 | — |
| `langgraph_sdk/schema.py` | `Assistant`/`Thread`/`ThreadState`/`Run`/`Cron`/`Item` 等 TypedDict，以及 `StreamMode`/`RunStatus` 等 Literal | 最高 —— 客户端期望的响应形状 |
| `docs/openapi.json` | 本项目导出的 spec（40 个端点） | 高 |
| `libs/aegra-api/src/` 源码 | 校验 OpenAPI 与实现不一致处 | 最高 |
| [docs.langchain.com](https://docs.langchain.com/langsmith/server-api-ref) | 补充确认资源分组、`supersteps`/`prune` 语义、thread 状态枚举 | 中 —— 官方未公开完整 OpenAPI JSON，需部署实例的 `/openapi.json` 才能取全 |

SDK 契约共 **51 个唯一 `(method, path)`**。A2A 与 MCP 端点组虽在官方文档中列出，但不在 SDK 数据面调用范围内，且已确认上游无人使用，本文不计入。

## 二、总览

| 资源 | SDK 端点 | 已实现 | 缺失 | 参数/响应不完整 |
|:--|--:|--:|--:|--:|
| Assistants | 12 | 11 | 1 | 4 |
| Threads | 14 | 10 | 4 | 4 |
| Runs | 14 | 12 | 2 | 4 |
| Crons | 6 | 6 | 0 | 4 |
| Store | 5 | 5 | 0 | 4 |
| **合计** | **51** | **44** | **7** | **20** |

端点覆盖率 86%（44/51）。但**端点存在 ≠ 功能可用** —— 20 个端点存在参数或响应字段缺口，其中 7 处会导致 SDK 调用静默失效（第六节）。

端点与参数的差集由 AST 机械比对得出而非人工通读，并逐项回到项目源码确认参数是否真正生效（方法与陷阱见第十节）。

图例：✅ 对齐 · ⚠️ 存在但不完整 · ❌ 缺失

## 三、端点级对比

### 3.1 Assistants

| SDK 方法 | 端点 | 状态 | 差距 |
|:--|:--|:-:|:--|
| `get()` | `GET /assistants/{id}` | ✅ | — |
| `get_graph()` | `GET /assistants/{id}/graph` | ✅ | 支持 `xray` |
| `get_schemas()` | `GET /assistants/{id}/schemas` | ⚠️ | 响应缺 `graph_id`、`context_schema`；四个 schema 声明为 required（SDK 侧全部 nullable） |
| `get_subgraphs()` | `GET /assistants/{id}/subgraphs` | ✅ | 支持 `recurse`、`namespace`（query 形式） |
| `get_subgraphs()` | `GET /assistants/{id}/subgraphs/{namespace}` | ❌ | **路径形式缺失**。SDK 传 `namespace` 参数时走这条路径 |
| `create()` | `POST /assistants` | ⚠️ | `if_exists` 默认值为 `"error"`，SDK 发 `"raise"`/`"do_nothing"`（行为等价，仅 OpenAPI 声明不符） |
| `update()` | `PATCH /assistants/{id}` | ✅ | — |
| `delete()` | `DELETE /assistants/{id}` | ⚠️ | 缺 `delete_threads` 参数 |
| `search()` | `POST /assistants/search` | ⚠️ | 缺 `select`；不返回 `X-Pagination-Next` 响应头，导致 `response_format="object"` 的 `next` 恒为 `None`（见 6.6）；`limit` 上限 100（SDK 允许 1000） |
| `count()` | `POST /assistants/count` | ✅ | — |
| `get_versions()` | `POST /assistants/{id}/versions` | ⚠️ | 无请求体，缺 `limit`、`offset`、`metadata` |
| `set_latest()` | `POST /assistants/{id}/latest` | ✅ | — |

### 3.2 Threads

| SDK 方法 | 端点 | 状态 | 差距 |
|:--|:--|:-:|:--|
| `get()` | `GET /threads/{id}` | ⚠️ | 缺 `include` 参数；**响应缺 `values`、`interrupts`**（见 6.2） |
| `create()` | `POST /threads` | ⚠️ | 缺 `supersteps`、`ttl`；存在非标准字段 `initial_state`。SDK 的 `graph_id` 被合并进 `metadata.graph_id`，项目接受 `metadata`，故该参数实际可用 |
| `update()` | `PATCH /threads/{id}` | ⚠️ | 缺 `ttl`、`return_minimal`（`Prefer` 头） |
| `delete()` | `DELETE /threads/{id}` | ✅ | — |
| `search()` | `POST /threads/search` | ⚠️ | 缺 `values`、`ids`、`select`、`extract`；`sort_by` 缺 `state_updated_at` |
| `count()` | `POST /threads/count` | ❌ | 缺失 |
| `copy()` | `POST /threads/{id}/copy` | ❌ | 缺失 |
| `prune()` | `POST /threads/prune` | ❌ | 缺失。官方用于回收 checkpoint 存储（DeltaChannel-aware），`strategy` 取 `PruneStrategy = delete｜keep_latest` |
| `get_state()` | `GET /threads/{id}/state` | ✅ | 支持 `subgraphs` |
| `get_state()` | `GET /threads/{id}/state/{checkpoint_id}` | ✅ | — |
| `get_state()` | `POST /threads/{id}/state/checkpoint` | ✅ | — |
| `update_state()` | `POST /threads/{id}/state` | ✅ | 支持 `values`、`as_node`、`checkpoint` |
| `get_history()` | `POST /threads/{id}/history` | ✅ | 支持 `limit`、`before`、`metadata`、`checkpoint` |
| `join_stream()` | `GET /threads/{id}/stream` | ❌ | 缺失。thread 级流式（跨 run 连续订阅） |

### 3.3 Runs

| SDK 方法 | 端点 | 状态 | 差距 |
|:--|:--|:-:|:--|
| `create()` | `POST /threads/{id}/runs` | ⚠️ | 缺 8 个参数（见 4.1） |
| `create()` | `POST /runs`（stateless） | ⚠️ | 同上，缺 8 个 |
| `stream()` | `POST /threads/{id}/runs/stream` | ⚠️ | 缺 9 个（多 `feedback_keys`） |
| `stream()` | `POST /runs/stream` | ⚠️ | 同上，缺 9 个 |
| `wait()` | `POST /threads/{id}/runs/wait` | ⚠️ | 缺 7 个（不含 `stream_resumable`） |
| `wait()` | `POST /runs/wait` | ⚠️ | 同上，缺 7 个 |
| `create_batch()` | `POST /runs/batch` | ❌ | 缺失 |
| `list()` | `GET /threads/{id}/runs` | ⚠️ | 缺 `select`；`limit` **无上限**（SDK 侧上限 1000） |
| `get()` | `GET /threads/{id}/runs/{run_id}` | ⚠️ | 响应缺 `metadata`、`multitask_strategy`（见 6.1） |
| `cancel()` | `POST /threads/{id}/runs/{run_id}/cancel` | ✅ | 支持 `wait`、`action` |
| `cancel_many()` | `POST /runs/cancel` | ❌ | 缺失。参数为 `thread_id`+`run_ids` 或 `status`（`BulkCancelRunsStatus = pending｜running｜all`）+ `action` |
| `join()` | `GET /threads/{id}/runs/{run_id}/join` | ✅ | — |
| `join_stream()` | `GET /threads/{id}/runs/{run_id}/stream` | ⚠️ | **参数名不匹配 + 未生效**（见 6.3） |
| `delete()` | `DELETE /threads/{id}/runs/{run_id}` | ✅ | 额外支持 `force` |

### 3.4 Crons

六个端点全部存在，差距集中在参数：

| SDK 方法 | 端点 | 状态 | 缺失参数 |
|:--|:--|:-:|:--|
| `create_for_thread()` | `POST /threads/{id}/runs/crons` | ⚠️ | `checkpoint_during`、`stream_resumable`、`durability` |
| `create()` | `POST /runs/crons` | ⚠️ | 同上 |
| `delete()` | `DELETE /runs/crons/{cron_id}` | ✅ | — |
| `update()` | `PATCH /runs/crons/{cron_id}` | ⚠️ | `stream_resumable`、`durability` |
| `search()` | `POST /runs/crons/search` | ⚠️ | `metadata`、`select` |
| `count()` | `POST /runs/crons/count` | ⚠️ | `metadata` |

### 3.5 Store

五个端点全部存在，差距集中在 TTL 与语义检索：

| SDK 方法 | 端点 | 状态 | 缺失参数/字段 |
|:--|:--|:-:|:--|
| `put_item()` | `PUT /store/items` | ⚠️ | `index`（按字段建向量索引）、`ttl` |
| `get_item()` | `GET /store/items` | ⚠️ | `refresh_ttl`；响应缺 `created_at`、`updated_at` |
| `delete_item()` | `DELETE /store/items` | ✅ | — |
| `search_items()` | `POST /store/items/search` | ⚠️ | `refresh_ttl`；响应缺 `score`（语义相关度） |
| `list_namespaces()` | `POST /store/namespaces` | ✅ | — |

## 四、请求参数缺失明细

### 4.1 Run 创建（影响 6 个端点）

`create()` / `stream()` / `wait()` 共用同一请求体，但发送的字段集不同：`create()` 21 个、`stream()` 23 个（多 `feedback_keys` 与 `on_disconnect`）、`wait()` 19 个。项目统一接受 15 个，缺失情况：

| 参数 | SDK 类型 | 语义 | 影响 | 影响方法 |
|:--|:--|:--|:--|:--|
| `webhook` | `str` | run 终态时 POST 最终 Run 载荷 | 传了不报错，但永不回调 | 全部 |
| `after_seconds` | `int` | 延迟指定秒数后开始 | 传了立即执行，延迟语义丢失 | 全部 |
| `if_not_exists` | `"create"｜"reject"` | 目标 thread 不存在时创建还是 404 | 传 `create` 仍 404 | 全部 |
| `durability` | `"sync"｜"async"｜"exit"` | checkpoint 持久化时机 | 无法控制持久化策略 | 全部 |
| `checkpoint_during` | `bool` | `durability` 的旧别名 | 同上 | 全部 |
| `checkpoint_id` | `str` | `checkpoint` 的扁平化形式 | 只能用嵌套 `checkpoint` 传 | 全部 |
| `langsmith_tracer` | 对象 | 客户端侧追踪配置 | 忽略 | 全部 |
| `stream_resumable` | `bool` | 事件是否落盘以支持断线重放 | 断线后无法重放 | `create`、`stream` |
| `feedback_keys` | `list[str]` | LangSmith 反馈键 | 忽略 | 仅 `stream` |

项目额外接受的非 SDK 字段：`stream`（布尔开关，SDK 用独立端点区分）。

其中 `multitask_strategy` 虽被接受并存入 `execution_params`，但 **无任何执行逻辑**（`services/multitask.py` 已移除），`reject`/`interrupt`/`rollback`/`enqueue` 四种策略全部静默失效。

### 4.2 Thread 创建与检索

| 参数 | 端点 | 语义 | 现状 |
|:--|:--|:--|:--|
| `supersteps` | `POST /threads` | 用一串顺序状态更新预填 thread（导入历史会话、构造测试场景） | 缺失；项目有非标准的 `initial_state` |
| `ttl` | `POST /threads`、`PATCH /threads/{id}` | 按 thread 设置保留策略 `{"strategy":"delete","ttl":分钟}` | 缺失（retention 整体已移除） |
| `graph_id` | `POST /threads` | 创建时绑定 graph | **可用** —— SDK 把它合并进 `payload["metadata"]["graph_id"]` 而非作为顶层字段发送，项目接受 `metadata` |
| `values` | `POST /threads/search` | 按状态内容过滤（JSONB 包含） | 缺失 |
| `ids` | `POST /threads/search` | 按 id 集合批量取 | 缺失 |
| `extract` | `POST /threads/search` | 从状态中投影指定路径 | 缺失 |
| `include` | `GET /threads/{id}` | 控制是否附带 state | 缺失 |
| `select` | search 系列 | 字段投影，减小响应体 | 全部缺失（assistants/threads/runs/crons） |
| `state_updated_at` | `sort_by` | 按状态更新时间排序 | 缺失 |

## 五、响应模型对比

| SDK 类型 | 期望字段 | 项目缺失 | 影响 |
|:--|:--|:--|:--|
| `Thread` | `thread_id`、`created_at`、`updated_at`、`metadata`、`status`、`values`、`interrupts`、`extracted` | **`values`、`interrupts`**、`extracted` | 高 —— 见 6.2 |
| `Run` | `run_id`、`thread_id`、`assistant_id`、`created_at`、`updated_at`、`status`、`metadata`、`multitask_strategy` | **`metadata`、`multitask_strategy`** | 高 —— 见 6.1 |
| `GraphSchema` | `graph_id`、`input_schema`、`output_schema`、`state_schema`、`config_schema`、`context_schema` | `graph_id`、`context_schema` | 中 —— 无法发现 graph 的 context 契约 |
| `Assistant` | `assistant_id`、`graph_id`、`config`、`context`、`created_at`、`updated_at`、`metadata`、`version`、`name`、`description` | 无缺失（`metadata` 经 `metadata_dict` 别名映射） | — |
| `ThreadState` | `values`、`next`、`checkpoint`、`metadata`、`created_at`、`parent_checkpoint`、`tasks`、`interrupts` | 无缺失 | — |
| `Cron` | 14 字段 | 无缺失 | — |
| `Item` | `namespace`、`key`、`value`、`created_at`、`updated_at` | `created_at`、`updated_at` | 低 |
| `SearchItem` | `Item` + `score` | `score` | 中 —— 语义检索无法排序 |
| `AssistantsSearchResponse` | `assistants`、`next`（分页游标，取自 `X-Pagination-Next` 头） | 不返回该响应头 | 中 —— 见 6.6 |

### 5.1 错误响应契约

SDK 的 `_map_status_error()` 按 HTTP 状态码映射异常类（400→`BadRequestError`、401→`AuthenticationError`、403→`PermissionDeniedError`、404→`NotFoundError`、409→`ConflictError`、422→`UnprocessableEntityError`、429→`RateLimitError`、≥500→`InternalServerError`）。项目的状态码使用与之一致。

错误消息由 `_extract_error_message()` 从响应体按 `message` → `detail` → `error` 顺序提取，**且要求值是非空字符串**，否则回退到 `"<status> <reason>"`。

| 项目响应形状 | 适用范围 | SDK 提取结果 |
|:--|:--|:--|
| `{error, message, details}`（`AgentProtocolError`） | 400/401/403/404/409/5xx | ✅ 命中 `message`，错误信息完整 |
| `{detail: [ValidationError, ...]}`（FastAPI 默认） | **422** | ❌ `detail` 是数组不是字符串，回退为通用文案（见 6.7） |

`APIError` 还会 best-effort 读响应体的 `code`、`param`、`type` 三个字段。项目的信封用 `error`/`details` 命名，故 `err.code`、`err.type` 恒为 `None` —— 属于 best-effort 范畴，不影响异常类型判断。

`err.request_id` 对齐：SDK 读 `x-request-id` 响应头，项目以默认配置挂载 `CorrelationIdMiddleware`（未覆盖 `header_name`），暴露的是 `X-Request-ID` —— httpx 的 header 查找大小写不敏感，故此项**已对齐**。

### 5.2 认证与传输层

SDK 从 `api_key` 参数或 `LANGSMITH_API_KEY` / `LANGCHAIN_API_KEY` 环境变量解析出 key，以 **`x-api-key`** 请求头发送。

项目不硬编码任何认证头，而是把认证委托给 `aegra.json` 的 `auth.path` 所指的 `@auth.authenticate` handler。因此对齐是**可配置的而非默认的**：要让 SDK 客户端开箱可用，自定义 handler 必须读取 `x-api-key` 头。这一点值得写进部署文档。

### 5.3 易误判为缺失、实际已对齐的项

以下几项在 SDK 侧有专门类型或机制，看字段名容易判成缺失，但读实现后确认可用。记录在此以免重复排查：

| 项 | SDK 机制 | 项目现状 |
|:--|:--|:--|
| `on_run_created` 回调 | 从 **`Content-Location`** 响应头正则提取 `RunCreateMetadata{run_id, thread_id}`，不是读响应体 | ✅ 五处流式/创建端点均设 `Content-Location: /threads/{id}/runs/{run_id}`，且在 CORS `expose_headers` 中暴露（浏览器可读）|
| `Config{tags, recursion_limit, configurable}` | 整个 config 传给运行时 | ✅ `create_run_config()` 对入参 `deepcopy` 后只增不删（仅注入 `configurable.thread_id`/`run_id` 与 `metadata`），故 `tags`、`recursion_limit` 原样透传 |
| `err.request_id` | 读 `x-request-id` 响应头 | ✅ `CorrelationIdMiddleware` 默认暴露 `X-Request-ID`，httpx 查找大小写不敏感 |
| `graph_id`（创建 thread） | 被 SDK 合并进 `metadata.graph_id` | ✅ 项目接受 `metadata`（见 4.2）|
| v2 流式协议 | 客户端转换，但依赖服务端 `type｜ns1｜ns2` 事件名约定 | ✅ 项目按该格式拼接（见 8.3）|
| `interrupt_before/after` 的 `"*"` | `All = Literal["*"]` | ✅ 支持全节点通配 |

## 六、静默失效清单（最高优先级）

以下七处**不报错、不 4xx**，客户端以为生效但实际无效。这类问题比端点缺失更危险 —— 缺失会拿到 404，静默失效拿到 200。

### 6.1 `Run.metadata` 与 `multitask_strategy` 不返回

SDK 的 `Run` TypedDict 把这两个字段列为一等成员。项目的 `Run` 响应模型不含它们，对应的数据库列也已从 ORM 移除。任何读 `run["metadata"]` 的客户端代码会 `KeyError`。

### 6.2 `Thread.values` / `interrupts` 不返回

SDK 的 `Thread` TypedDict 含 `values`（当前状态）和 `interrupts`（task_id → Interrupt 列表）。项目的 `Thread` 响应只有 6 个字段，两者都缺。

影响面最大的一处：Agent Chat UI 一类客户端依赖 `thread.values` 直接渲染会话历史，依赖 `thread.interrupts` 判断是否需要人工介入。缺这两个字段时，客户端必须对每个 thread 额外调 `GET /threads/{id}/state`，N+1 请求，且 `POST /threads/search` 的批量列表页无法渲染。

### 6.3 `join_stream` 的参数完全未生效

| | SDK 发送 | 项目声明 |
|:--|:--|:--|
| 流式模式 | `?stream_mode=values` | `?_stream_mode=`（下划线前缀，名字对不上） |
| 断连取消 | `?cancel_on_disconnect=true` | 未声明 |

即便名字对上，`_stream_mode` 在 [runs.py:371](../libs/aegra-api/src/aegra_api/api/runs.py#L371) 只出现在函数签名里，**函数体从未使用它**。所以 `client.runs.join_stream(..., stream_mode=["values"], cancel_on_disconnect=True)` 能连上流，但模式过滤和断连取消双双无效。

### 6.4 `multitask_strategy` 无执行逻辑

见 4.1 末段。四种 double-texting 策略全部静默失效，同一 thread 的并发 run 没有任何串行化保护。

### 6.5 被静默忽略的请求字段

Pydantic 模型未设 `extra="forbid"`，4.1 表中的 9 个参数传入后既不报错也不生效。`webhook` 最典型：客户端配了回调地址，run 完成后什么都不发，且无从察觉。

### 6.6 分页游标 `X-Pagination-Next` 不返回

`response_format` 是纯客户端参数，不发给服务端：SDK 在 `response_format="object"` 时挂一个响应回调读 `X-Pagination-Next` 头，组装成 `{"assistants": [...], "next": cursor}`。

项目源码中完全没有这个头（`grep -r X-Pagination-Next` 无结果），所以 `next` 恒为 `None`，客户端无法翻页，只能靠 `offset` 自行推进。服务端要做的只是在 search 响应上补这个头。

### 6.7 422 校验错误的具体原因丢失

项目的 422 走 FastAPI 默认信封 `{"detail": [{...}]}`，而 SDK 的 `_extract_error_message()` 要求 `detail` 是**字符串**。数组不匹配，于是回退到 `"422 Unprocessable Entity"` —— 客户端拿不到「哪个字段错了」，只能看到通用文案。

其余状态码走项目自己的 `{error, message, details}` 信封，`message` 能被正确提取。所以问题只出在 422：把校验错误压成一条字符串放进 `message`、明细保留在 `details`，即可对齐（这正是 0.15.0 里 `validation_exception_handler` 的做法，本次回退将其移除）。

## 七、OpenAPI 声明偏差

功能可用但对外声明与 Platform 不一致，会影响基于 spec 生成客户端的工具（LangGraph Studio 等）：

| 位置 | 声明 | SDK 实际 | 后果 |
|:--|:--|:--|:--|
| `POST /threads` | `threadId`、`ifExists`（camelCase 别名） | `thread_id`、`if_exists` | 模型有 `populate_by_name=True`，snake_case 能被接受，仅 spec 不准 |
| `POST /assistants` | `if_exists` 默认 `"error"` | `"raise"` | 行为等价（都走 409 分支），仅枚举值不符 |
| `GET /assistants/{id}/schemas` | 四个 schema 为 required | 全部 nullable | graph 未暴露某个 schema 时构造响应会 500 |
| `POST /threads/search` | 同时有 `order_by` 与 `sort_by` | 只有 `sort_by` | 冗余字段 |
| `GET /threads/{id}/runs` | `limit` 无上限 | 上限 1000 | 可传任意大值，DoS 面 |

## 八、流式协议对比

SDK 定义了**两套彼此独立**的流式模式枚举，对应两类端点：`StreamMode` 用于 run 级流式，`ThreadStreamMode` 用于 thread 级流式。

### 8.1 run 级 `StreamMode`

| 模式 | SDK 定义 | 项目 | 说明 |
|:--|:-:|:-:|:--|
| `values` | ✅ | ✅ | 默认模式 |
| `updates` | ✅ | ✅ | |
| `messages` | ✅ | ✅ | |
| `messages-tuple` | ✅ | ✅ | Python graph 会被归一化为 `messages` |
| `custom` | ✅ | ✅ | 透传 LangGraph |
| `debug` | ✅ | ✅ | 始终强制开启 |
| `events` | ✅ | ✅ | |
| `tasks` | ✅ | ⚠️ | 透传 LangGraph，未见专门事件构造 |
| `checkpoints` | ✅ | ⚠️ | 同上 |

项目侧的合法性校验已随 `_parse_stream_modes` 移除，非法 `stream_mode` 不再返回 422。

另注：`langgraph` 核心库自己的 `StreamMode` 只有 7 个值（无 `events`、`messages-tuple`）—— 这两个是 API 层概念，由 Agent Server 翻译成核心库的模式，项目已实现该翻译（`messages-tuple` 对 Python graph 归一化为 `messages`）。

### 8.2 thread 级 `ThreadStreamMode`

`GET /threads/{id}/stream` 用的是另一套枚举，与 run 级完全不重叠：

| 模式 | 语义 | Platform | 项目 |
|:--|:--|:-:|:-:|
| `run_modes` | 转发该 thread 上各 run 的 run 级事件 | ✅ | ❌ |
| `lifecycle` | thread 生命周期事件（run 开始/结束等） | ✅ | ❌ |
| `state_update` | thread 状态变更事件 | ✅ | ❌ |

三者均不可用，因为端点本身缺失（3.2）。补齐时需注意：0.15.0 曾实现过该端点，但只支持 `run_modes` 一种模式（源码中有 `Only the 'run_modes' thread stream mode is supported` 的 422），即便直接恢复那份实现，也只覆盖 1/3。

### 8.3 SSE 事件类型与协议版本（v1 / v2）

SDK 的 `runs.stream()` 有 `version: Literal["v1","v2"]` 参数（默认 `v1`），两个版本的事件形状不同：

| | v1（`StreamPart`） | v2（`StreamPartV2`） |
|:--|:--|:--|
| 形状 | `(event, data, id)` 三元组 | `{"type", "ns", "data"}` 字典 |
| 子图层级 | 编码在 `event` 名里 | 拆成 `ns: list[str]` |
| 流结束 | `end` 事件 | 转换器返回 `None`（吞掉该事件）|
| 中断 | 需自行从 data 里找 | `values` 类型额外带 `interrupts` 字段 |

**`version` 不发给服务端** —— 它是纯客户端转换（`_wrap_stream_v2` → `_sse_to_v2_dict`）。但转换**依赖服务端的事件名约定**：

```
event.split("|")  →  parts[0] 作为 type，parts[1:] 作为 ns
```

即子图事件的 SSE 事件名必须形如 `values|node_a|inner`，客户端才能解出 `ns: ["node_a", "inner"]`。

**项目已满足该约定**：`graph_streaming.py` 在 `stream_subgraphs=True` 时拼 `f"{mode}|{ns_str}"`（`ns_str = "|".join(namespace)`）。因此 v1 与 v2 均可用，**此项已对齐** ✅。

v2 定义了 11 个具体事件类型（`values`、`updates`、`messages`、`messages/partial`、`messages/complete`、`messages/metadata`、`custom`、`checkpoints`、`tasks`、`debug`、`metadata`）。项目实际发出：`metadata`、`values`、`updates`、`messages`（含三个 `messages/*` 变体）、`debug`、`end`、`error` —— 与 v1 解码器兼容；`checkpoints`、`tasks` 两类事件透传 LangGraph 但无专门构造（与 8.1 表中的 ⚠️ 一致）。

> **勿与项目的 `event_streaming_v2` 混淆。** 后者是 Aegra 自有的双向命令协议（`services/event_streaming/protocol.py` 构造 `{"type":"event","seq","method","params"}` 与 `{"type":"success","id","result"}`），服务于两个非 SDK 端点 `POST /threads/{id}/commands` 和 `POST /threads/{id}/stream/events`，与 SDK 的 v2 流式格式无关。

### 8.4 断线重放

| 能力 | Platform | 项目 |
|:--|:-:|:-:|
| `Last-Event-ID` 头恢复 | ✅ | ✅ |
| `"-"` 从头重放 | ✅ | ⚠️ 未显式识别该值，但 `replay()` 找不到匹配 id 时会 fallback 返回全部事件，效果等价 —— 依赖 fallback 而非契约，且仅在事件仍在 buffer 内时成立 |
| `stream_resumable` 落盘 | ✅ | ❌ 参数未接受 |
| thread 级 join（`GET /threads/{id}/stream`） | ✅ | ❌ 端点缺失 |

## 九、补齐建议（按投入产出排序）

### P0 —— 静默失效，客户端无从察觉

1. **`Thread` 响应补 `values` + `interrupts`**（6.2）。影响面最大，直接决定 Agent Chat UI 类客户端能否工作。需要在 thread 读取路径上带出 checkpointer 状态。
2. **`Run` 响应补 `metadata` + `multitask_strategy`**（6.1）。需要恢复对应数据库列 —— 迁移 `d1f7b3a9c5e2` 已经加过这两列且当前仍在库中，只需在 ORM 与响应模型上接回来。
3. **修 `join_stream` 参数**（6.3）。把 `_stream_mode` 改名为 `stream_mode` 并在函数体内实际使用，补 `cancel_on_disconnect`。改动量小、收益直接。
4. **恢复 422 的 Agent Protocol 信封**（6.7）。把校验错误压成字符串放进 `message`、明细留在 `details`，SDK 才能提取出具体原因。0.15.0 的 `validation_exception_handler` 是现成实现。
5. **search 响应补 `X-Pagination-Next` 头**（6.6）。只需加一个响应头，`response_format="object"` 的分页游标就能工作。
6. **给已移除的请求字段一个明确的错误**（6.5）。要么实现，要么 `extra="forbid"` 返回 422 —— 静默忽略是最差选项。

### P1 —— 端点缺失，客户端拿到 404

7. `POST /threads/count`、`POST /threads/{id}/copy` —— 实现简单，SDK 有对应方法。
8. `GET /assistants/{id}/subgraphs/{namespace}` —— query 形式已实现，补一条路径路由即可。
9. `POST /runs/cancel`（批量取消）、`POST /runs/batch`（批量创建）。
10. `select` 字段投影（assistants/threads/runs/crons 四处 search）—— 大列表场景的响应体优化。

### P2 —— 功能族缺失，需要设计

11. **`multitask_strategy` 执行逻辑**（6.4）。这是 Platform 的核心并发语义，需要 double-texting 串行化。注意：迁移在库里留下了 `uq_runs_one_running_per_thread` 唯一索引，实现前先确认它已被 `f2c8a5e13d94` 删除，否则并发 run 会撞唯一约束。
12. **`webhook` 回调**。Platform 语义是 run 终态 POST 最终 Run 载荷，需要事务性 outbox 才能保证不丢。
13. **`after_seconds` 延迟运行** + `durability` 持久化策略。
14. **Thread TTL / `POST /threads/prune`** —— retention 功能族，配合 checkpoint 存储回收。
15. **Store `index` / `ttl` / `refresh_ttl` / `score`** —— 语义检索与过期，依赖 `AsyncPostgresStore` 的 index/ttl 配置。
16. **`supersteps`** 预填 thread 状态。
17. **`GET /threads/{id}/stream`** thread 级流式 + `stream_resumable` 事件落盘。注意要覆盖 `ThreadStreamMode` 的三种模式（`run_modes`/`lifecycle`/`state_update`），0.15.0 的实现只做了第一种。

## 十、如何重做这个分析

升级 `langgraph-sdk` 后，用 AST 解析 `.venv/Lib/site-packages/langgraph_sdk/_async/{assistants,runs,threads,cron,store}.py`（顶层 `LangGraphClient` 只组装这五个子客户端，没有额外 HTTP 方法；`_sync` 与 `_async` 端点集合已验证一致），找出所有 `self.http.<verb>(...)` 调用；项目侧先 `make openapi` 再解析 `docs/openapi.json`。两边取差集即可。

以下五个坑是实际踩过的，直接按朴素方式提取会得出错误结论：

1. **`@overload` 方法要取最后一个同名定义。** `assistants.search`、`threads.update`、`runs.create/stream/wait` 都有重载声明，声明体是 `...`，提取出来是空集。漏了这一步会以为这些端点「没有任何参数」。
2. **路径可能是三元表达式或变量。** `create()`/`stream()`/`wait()` 的路径形如 `f"/threads/{id}/runs" if thread_id else "/runs"`，必须展开成两条；`stream()`/`wait()` 还先赋值给 `endpoint` 变量再传入，需回溯变量赋值。不展开会少算 5 个端点（46 而非 51）。
3. **动词不能按方法名推断。** `join()`/`join_stream()` 走 `http.request_reconnect`/`http.stream`，真实动词在 `method=` 实参里 —— 这两个是 `GET`，而 `stream()`/`wait()`/`cancel()` 是 `POST`。
4. **嵌套键会混进请求字段。** `supersteps` 内部的 `updates`/`values`/`as_node`/`command` 和 `ttl` 内部的 `strategy` 都是嵌套结构的键，不是顶层字段。按 dict 字面量键统计会虚增缺失项。
5. **有些「缺失」其实可用，有些「参数」根本不发给服务端。** `graph_id` 被 SDK 合并进 `metadata`（3.2）；`response_format` 是纯客户端参数，服务端只需返回 `X-Pagination-Next` 头（6.6）。这两类必须读 SDK 实现才能判断，不能只比字段名。

判定 ✅/⚠️/❌ 时，端点存在性可以机械比对，但**参数是否真正生效必须读项目实现** —— 6.3 的 `_stream_mode` 就是签名里声明了、函数体从未使用的例子，只看 OpenAPI 会误判为「已支持」。
