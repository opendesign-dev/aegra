# LangSmith Platform 数据面对齐现状

> 分析对象：`feat/runtime-hardening`，aegra-api 0.17.0
> 对齐目标：LangSmith Deployment / Agent Server 数据面 API
> 更新日期：2026-08-01（上一版 2026-07-31 记录的差距已按本文第五节落地）

## 一、依据与方法

对齐的判定标准是「用官方 `langgraph-sdk` 的客户端能否正常工作」，所以以 SDK 的实际 HTTP 调用为准，而非官方文档描述。

| 来源 | 说明 | 权威性 |
|:--|:--|:--|
| `langgraph-sdk 0.4.2` 源码 | `.venv/Lib/site-packages/langgraph_sdk/_async/{assistants,runs,threads,cron,store}.py`，逐方法核对动词、路径、请求字段 | 最高 —— 这是客户端真实发出的请求 |
| `langgraph_sdk/schema.py` | `Assistant`/`Thread`/`ThreadState`/`Run`/`Cron`/`Item` 等 TypedDict，以及 `StreamMode`/`RunStatus` 等 Literal | 最高 —— 客户端期望的响应形状 |
| `docs/openapi.json` | 本项目导出的 spec（47 条路径 / 61 个操作） | 高 |
| `libs/aegra-api/src/` 源码 | 校验 OpenAPI 与实现不一致处 | 最高 |
| [docs.langchain.com](https://docs.langchain.com/langsmith/server-api-ref) | 补充确认资源分组、`supersteps`/`prune` 语义、thread 状态枚举 | 中 —— 官方未公开完整 OpenAPI JSON |

SDK 契约共 **51 个唯一 `(method, path)`**。A2A 与 MCP 端点组虽在官方文档中列出，但不在 SDK 数据面调用范围内，本文不计入。

两边都按 `(method, path)` 计数才可比：spec 的 61 个操作里，4 个是基础设施（`/health`、`/live`、`/ready`、`/info`）、1 个根路径、3 个示例自定义路由，余下 **57 = SDK 数据面 51 + Aegra 自有 6**。这 6 个不在 SDK 契约里，重做分析时会被差集算作「多余」，是预期的：

| Aegra 自有端点 | 用途 |
|:--|:--|
| `GET /assistants`、`GET /threads` | 不分页的全量列表，早于 `search` 存在 |
| `PATCH /threads/{id}/runs/{run_id}` | 改 run 状态（SDK 用 `/cancel`） |
| `GET /threads/{id}/history` | `POST` 版的 query-param 便捷形式 |
| `POST /threads/{id}/commands`、`POST /threads/{id}/stream/events` | Aegra 自有的双向命令协议（见 9.3 末的提醒） |

## 二、总览

| 资源 | SDK 端点 | 已实现 | 缺失 | 参数/响应不完整 |
|:--|--:|--:|--:|--:|
| Assistants | 12 | 12 | 0 | 0 |
| Threads | 14 | 14 | 0 | 0 |
| Runs | 14 | 14 | 0 | 0 |
| Crons | 6 | 6 | 0 | 0 |
| Store | 5 | 5 | 0 | 0 |
| **合计** | **51** | **51** | **0** | **0** |

端点覆盖率 100%。SDK 会发送的每个请求字段现在都落在三类之一：**生效**、**记录并在字段说明里写明其语义边界**、**4xx/501 拒绝**。没有静默忽略项 —— 这是上一版分析的头号问题（客户端拿到 200 却以为选项生效）。

第四节列出四处语义边界，其中两处是 `langgraph-checkpoint-postgres` 尚未实现的 checkpointer 方法所致（4.4），依赖升级后自动恢复。

图例：✅ 对齐 · ⚠️ 有语义边界（见备注） · ❌ 缺失

## 三、端点级对比

### 3.1 Assistants

| SDK 方法 | 端点 | 状态 | 备注 |
|:--|:--|:-:|:--|
| `get()` | `GET /assistants/{id}` | ✅ | |
| `get_graph()` | `GET /assistants/{id}/graph` | ✅ | 支持 `xray` |
| `get_schemas()` | `GET /assistants/{id}/schemas` | ✅ | 含 `graph_id`、`context_schema`；五个 schema 均可空 |
| `get_subgraphs()` | `GET /assistants/{id}/subgraphs` | ✅ | query 形式 |
| `get_subgraphs()` | `GET /assistants/{id}/subgraphs/{namespace}` | ✅ | 路径形式，SDK 传 `namespace` 时走这条 |
| `create()` | `POST /assistants` | ✅ | `if_exists` 取 `raise`/`do_nothing` |
| `update()` | `PATCH /assistants/{id}` | ✅ | |
| `delete()` | `DELETE /assistants/{id}` | ✅ | 支持 `delete_threads` |
| `search()` | `POST /assistants/search` | ✅ | 支持 `select`；满页返回 `X-Pagination-Next`；`limit` 上限 1000 |
| `count()` | `POST /assistants/count` | ✅ | |
| `get_versions()` | `POST /assistants/{id}/versions` | ✅ | 支持 `limit`、`offset`、`metadata` |
| `set_latest()` | `POST /assistants/{id}/latest` | ✅ | |

### 3.2 Threads

| SDK 方法 | 端点 | 状态 | 备注 |
|:--|:--|:-:|:--|
| `get()` | `GET /threads/{id}` | ✅ | 响应含 `values`、`interrupts`；支持 `include=ttl` |
| `create()` | `POST /threads` | ✅ | 支持 `supersteps`、`ttl`；`graph_id` 经 `metadata` 传入 |
| `update()` | `PATCH /threads/{id}` | ✅ | 支持 `ttl` |
| `delete()` | `DELETE /threads/{id}` | ✅ | |
| `search()` | `POST /threads/search` | ✅ | 支持 `values`、`ids`、`select`、`extract`、`sort_by=state_updated_at` |
| `count()` | `POST /threads/count` | ✅ | |
| `copy()` | `POST /threads/{id}/copy` | ⚠️ | 复制实体 + 最新状态；checkpoint 历史取决于 checkpointer 能力（见 4.4） |
| `prune()` | `POST /threads/prune` | ⚠️ | 返回 `{"pruned_count": N}`；`keep_latest` 对存在待处理中断的 thread 跳过（见 4.2） |
| `get_state()` | `GET /threads/{id}/state` | ✅ | 支持 `subgraphs` |
| `get_state()` | `GET /threads/{id}/state/{checkpoint_id}` | ✅ | |
| `get_state()` | `POST /threads/{id}/state/checkpoint` | ✅ | |
| `update_state()` | `POST /threads/{id}/state` | ✅ | 支持 `values`、`as_node`、`checkpoint` |
| `get_history()` | `POST /threads/{id}/history` | ✅ | 支持 `limit`、`before`、`metadata`、`checkpoint` |
| `join_stream()` | `GET /threads/{id}/stream` | ⚠️ | 三种 `ThreadStreamMode` 全支持；空闲超时后关闭（见 4.3） |

### 3.3 Runs

| SDK 方法 | 端点 | 状态 | 备注 |
|:--|:--|:-:|:--|
| `create()` | `POST /threads/{id}/runs` | ✅ | 完整请求契约见 3.6 |
| `create()` | `POST /runs`（stateless） | ✅ | 同上 |
| `stream()` | `POST /threads/{id}/runs/stream` | ✅ | |
| `stream()` | `POST /runs/stream` | ✅ | |
| `wait()` | `POST /threads/{id}/runs/wait` | ✅ | |
| `wait()` | `POST /runs/wait` | ✅ | |
| `create_batch()` | `POST /runs/batch` | ✅ | 顺序创建，响应顺序与请求一致 |
| `list()` | `GET /threads/{id}/runs` | ✅ | 支持 `select`、`metadata`（JSON 对象，JSONB containment）；`limit` 上限 1000；满页返回 `X-Pagination-Next` |
| `get()` | `GET /threads/{id}/runs/{run_id}` | ✅ | 响应含 `metadata`、`multitask_strategy` |
| `cancel()` | `POST /threads/{id}/runs/{run_id}/cancel` | ✅ | 支持 `wait`、`action` |
| `cancel_many()` | `POST /runs/cancel` | ⚠️ | `thread_id`+`run_ids` 或 `status`；`action=rollback` 依赖 checkpointer 能力（见 4.4） |
| `join()` | `GET /threads/{id}/runs/{run_id}/join` | ✅ | |
| `join_stream()` | `GET /threads/{id}/runs/{run_id}/stream` | ✅ | `stream_mode` 真正过滤事件，`cancel_on_disconnect` 生效 |
| `delete()` | `DELETE /threads/{id}/runs/{run_id}` | ✅ | 额外支持 `force` |

### 3.4 Crons

| SDK 方法 | 端点 | 状态 | 备注 |
|:--|:--|:-:|:--|
| `create_for_thread()` | `POST /threads/{id}/runs/crons` | ✅ | 支持 `checkpoint_during`、`stream_resumable`、`durability`；`cron_id` 可选，重复返回 409 |
| `create()` | `POST /runs/crons` | ✅ | 同上 |
| `delete()` | `DELETE /runs/crons/{cron_id}` | ✅ | |
| `update()` | `PATCH /runs/crons/{cron_id}` | ✅ | 同上 |
| `search()` | `POST /runs/crons/search` | ✅ | 支持 `metadata`、`select`；满页返回 `X-Pagination-Next` |
| `count()` | `POST /runs/crons/count` | ✅ | 支持 `metadata` |

`CronResponse` 补上 `timezone` 后与 SDK 的 `Cron` 14 字段一致。`_build_payload` 与 `_build_run_create` 的字段集保持同步 —— 只在前者接受而不在后者转发，等于每次定时触发都静默丢用户的值。

cron 的 `metadata` 按 SDK 语义（"metadata to assign to the cron job runs"）由每次触发的 run 继承，并叠加服务端写入的 `cron_id`，因此「这条 schedule 产出的全部 run」就是 `GET /threads/{id}/runs?metadata={"cron_id":...}` 一次查询。由于值最终落在 run 上，cron metadata 在创建/更新时就按 run 的规则校验（键 31 个上限，第 32 个槽位留给 `cron_id`），而不是拖到几小时后首次触发才失败。

### 3.5 Store

| SDK 方法 | 端点 | 状态 | 备注 |
|:--|:--|:-:|:--|
| `put_item()` | `PUT /store/items` | ✅ | 支持 `index`、`ttl` |
| `get_item()` | `GET /store/items` | ✅ | 支持 `refresh_ttl`；响应含 `created_at`、`updated_at` |
| `delete_item()` | `DELETE /store/items` | ✅ | |
| `search_items()` | `POST /store/items/search` | ✅ | 支持 `refresh_ttl`；响应含 `score` |
| `list_namespaces()` | `POST /store/namespaces` | ✅ | |

`index` 与 `ttl` 仅在显式给出时下传，`False`/`None` 语义不同，省略才能让 store 自身的配置默认值继续生效。

### 3.6 Run 创建请求契约

`create()` / `stream()` / `wait()` 共用一个请求体但发送的字段集不同：`stream()` 23 个、`create()` 21 个、`wait()` 19 个，并集 23 个。`RunCreate` 现在全部接受，无缺口。上一版有 9 个是「接受但静默丢弃」，这里单独列出它们的落地方式：

| 参数 | SDK 类型 | 落地方式 |
|:--|:--|:--|
| `webhook` | `str` | run 到终态时，outbox 行与终态同事务落库，sweeper 投递（见 五.12） |
| `after_seconds` | `int` | 写入 `runs.scheduled_at`，`execute_run` 内等待（见 4.1） |
| `if_not_exists` | `create｜reject` | `reject` 时目标 thread 不存在即 404；默认仍是自动创建 |
| `durability` | `sync｜async｜exit` | 下传 `astream(durability=...)` |
| `checkpoint_during` | `bool` | 已弃用别名：`True` → `async`，`False` → `exit`；显式 `durability` 优先 |
| `checkpoint_id` | `str` | 折叠进嵌套的 `checkpoint`，不覆盖同时传入的 `checkpoint_ns` |
| `stream_resumable` | `bool` | 事件进 run 的重放缓冲，配合 `Last-Event-ID`（内存级，见 4.4 末） |
| `feedback_keys` | `list[str]` | 记入 `runs.metadata`，供 tracing 后端取用 |
| `langsmith_tracer` | 对象 | 同上。这两项按 SDK 定义是客户端侧 tracer 配置，服务端无可执行语义，故记录而非执行 |

`multitask_strategy` 此前也被接受并存入 `execution_params`，但四种策略全无执行逻辑；现已实现（见 五.11）。

项目额外接受一个非 SDK 字段：`stream`（布尔开关；SDK 用独立端点区分流式与否）。枚举类字段一律由 `models/enums.py` 的 Literal 校验，非法值 422 —— 不再出现「传了没报错也没生效」。

## 四、语义边界

四处行为与 Platform 一致但有实现层面的取舍，写在这里而不是留给使用者踩：

### 4.1 `after_seconds` 占用执行槽位

延迟在 `execute_run` 内部等待，好处是 dev 与 prod 两种模式共用一条路径、不需要额外的调度器；代价是 worker 模式下等待期间占一个并发槽。短延迟无影响，长延迟应改用 cron。

### 4.2 `prune(strategy="keep_latest")` 跳过有中断的 thread

`keep_latest` 的实现是「读出最新状态 → `adelete_thread` → 用 `aupdate_state` 重新落一个 checkpoint」。中断只能在抛出它的那个 checkpoint 上恢复，所以存在待处理中断的 thread 会被跳过而不是被压缩 —— 否则那次 run 就再也接不上了。这类 thread 不计入 `pruned_count`。

### 4.3 thread 级流式是轮询 + 空闲关闭

`GET /threads/{id}/stream` 建立在按 run 分键的 broker 之上：轮询该 thread 的下一个 run 并转发。`THREAD_STREAM_IDLE_TIMEOUT_SECONDS`（默认 300 秒）无新 run 即关闭，避免被遗弃的订阅长期占住连接。

### 4.4 两项能力受 checkpointer 限制

`langgraph-checkpoint-postgres` 2.x 的 `AsyncPostgresSaver` 只实现了 `adelete_thread`；`acopy_thread` 与 `adelete_for_runs` 仍是 `BaseCheckpointSaver` 里抛 `NotImplementedError` 的声明。`db_manager.supports()` 按「子类是否覆盖了基类方法」探测，两处据此分别处理：

| 能力 | 依赖方法 | 当前行为 |
|:--|:--|:--|
| `POST /threads/{id}/copy` | `acopy_thread` | 降级为「实体 + 最新物化状态」，记一条 info 日志。复制出的 thread 可读可续跑，只是没有历史 checkpoint。 |
| `rollback`（`multitask_strategy` 与 `POST /runs/cancel?action=rollback`） | `adelete_for_runs` | **501**，且在中断任何 run 之前就拒绝。 |

两者取舍不同是刻意的：copy 降级后仍交付一个可用的 thread；而 rollback 的全部意义就是丢弃状态，静默保留等于假装成功 —— 那正是本次要消除的失效模式。依赖升级后两处都会自动恢复完整语义，无需改代码。

另有一项按 SDK 定义即为客户端侧行为、服务端只需守约的：`stream_resumable` 的重放依赖 run 的内存事件缓冲（配合 `Last-Event-ID`），不跨进程重启。字段说明里已写明。

## 五、本次落地清单

对照上一版分析的三档优先级：

**P0 —— 静默失效**

1. `Thread` 响应补 `values` + `interrupts`。新增 `thread_state` 物化缓存（`services/thread_state_cache.py`），在 run 结束、显式状态更新、superstep 预填三处刷新；列表端点一次批量读取，不产生 N+1。
2. `Run` 响应补 `metadata` + `multitask_strategy`，ORM 接回 `d1f7b3a9c5e2` 已建的两列。
3. `join_stream` 的 `_stream_mode` 更名为 `stream_mode` 并真正过滤事件（`core/sse.py:filter_stream_modes`），补 `cancel_on_disconnect`。
4. 恢复 422 的 Agent Protocol 信封：校验错误压成字符串放进 `message`，明细留在 `details.errors`。
   同时修掉一个只在自定义 app 部署下暴露的隐性回归：`merge_exception_handlers` 把 FastAPI 自带的默认 handler 误判为「用户已覆盖」而跳过注册，于是配了 `http.app` 的部署（含本仓库默认的 `aegra.json`）仍返回 FastAPI 的列表形 `detail`。单元测试直接注册 handler，测不出这条 —— 是跑真实服务器时才发现的。
5. search 响应补 `X-Pagination-Next`（`core/query.py:set_next_page`）。
6. 消除静默忽略：`webhook`、`after_seconds`、`if_not_exists`、`durability`、`checkpoint_during`、`checkpoint_id`、`stream_resumable`、`feedback_keys`、`langsmith_tracer` 全部接受并生效或记录，非法枚举值 422。

**P1 —— 端点缺失**

7. `POST /threads/count`、`POST /threads/{id}/copy`、`POST /threads/prune`。
8. `GET /assistants/{id}/subgraphs/{namespace}`。
9. `POST /runs/cancel`、`POST /runs/batch`。
10. `select` 字段投影（assistants/threads/runs/crons 四处），`extract` 路径投影（threads）。

**P2 —— 功能族**

11. `multitask_strategy` 四种策略（`services/multitask.py`）：`reject` 409；`interrupt` 停在途 run；`rollback` 停并丢弃其 checkpoint（受 4.4 限制）；`enqueue` 以 `runs.dispatched` 列排队，前一个 run 终结时交接。
12. `webhook` 回调（`services/webhooks.py`）：outbox 行与 `finalize_run` 同事务落库，sweeper 以指数退避投递。
13. `after_seconds` + `durability`（后者经 `astream(durability=...)` 下传）。
14. Thread TTL 列与 `POST /threads/prune`。
15. Store `index`/`ttl`/`refresh_ttl`/`score`/时间戳。
16. `supersteps` 预填。
17. `GET /threads/{id}/stream`，覆盖 `run_modes`/`lifecycle`/`state_update` 三种模式。

**schema 变更**

迁移 `a3d6e0b95f17`（revises `f2c8a5e13d94`），三项全部是追加式：

- `runs.dispatched BOOLEAN NOT NULL DEFAULT true` —— `enqueue` 排队期间为 `false`。交接用的是这一列上的条件 UPDATE，这才让同一 thread 上两个 run 并发终结时不会重复派发同一个排队 run。默认 `true` 使既有行读作「已派发」。
- `idx_runs_thread_queued` —— 上述队列查询的部分索引。
- 重建 `f2c8a5e13d94` 当时按「无查询方」删掉的两个 GIN 索引（`idx_thread_state_values_gin`、`idx_cron_metadata_gin`）—— 它们现在分别服务 `POST /threads/search` 的 `values` 过滤与 cron 的 `metadata` 过滤。

`d1f7b3a9c5e2` 建的 `runs.metadata`、`runs.multitask_strategy`、`runs.scheduled_at`、`thread.ttl`、`thread_state`、`webhook_deliveries` 都还在库里，本次只是在 ORM 上重新声明，没有新建。

迁移 `b5e1c47a9d38`（revises `a3d6e0b95f17`）补两个索引，同样是纯追加：

- `idx_runs_metadata_gin` —— `GET /threads/{id}/runs` 的 `metadata` containment 过滤的查询方；cron 触发的 run 带 `cron_id`，按 schedule 拉取历史全靠它。
- `idx_webhook_deliveries_due` —— `f2c8a5e13d94` 随投递功能一起删掉，投递恢复后 sweeper 的 `(status, next_attempt_at)` 认领又需要它，ORM 也重新声明了。

## 六、升级注意（0.16.0 → 0.17.0）

minor 而非 patch，因为带 schema 迁移，且有两处请求契约收紧：

| 变更 | 影响 | 迁移动作 |
|:--|:--|:--|
| `POST /threads/search` 移除 `order_by` | 已标记 deprecated 的旧单字段形式（`"updated_at ASC"`），现在 422 | 改用 `sort_by` + `sort_order` |
| 四处 search 的 `limit`/`offset` 由 `int \| None` 收为 `int` | 显式传 `null` 现在 422（此前回落到默认值） | 省略该字段，或给具体数字。SDK 始终发整数，不受影响 |
| `POST /threads/prune` 响应形状 | `{"pruned": [...]}` → `{"pruned_count": N}` | 该端点未随 tag 发布过，仅影响跟着未发布分支走的调用方 |
| `Thread` 新增 `values`/`interrupts`、`Run` 新增 `metadata`/`multitask_strategy`、`Cron` 新增 `timezone`、store item 新增时间戳与 `score` | 追加字段，旧客户端忽略即可 | 无 |
| 五个端点改为 `response_model=None` | `select`/`include` 让行形状动态化，spec 上这些操作不再声明具体 schema：`POST /assistants/search`、`POST /threads/search`、`GET /threads/{id}`、`GET /threads/{id}/runs`、`POST /runs/crons/search` | 按 spec 生成客户端的工具需重新生成 |

多实例部署照旧：`RUN_MIGRATIONS_ON_STARTUP=false` + 带外执行 `aegra db upgrade`。迁移里的并发索引构建都在 `autocommit_block` 内、且带 `IF [NOT] EXISTS` 守卫，可重入。

## 六之二、升级注意（0.17.0 → 0.18.0）

同样是 minor：带 schema 迁移 `b5e1c47a9d38`，且有三处请求契约收紧。

| 变更 | 影响 | 迁移动作 |
|:--|:--|:--|
| `RunCreate.command` 由 `dict` 收为 `RunCommand`（`extra="forbid"`） | 只认 `goto`/`update`/`resume`，且三者至少有一个；拼错的键此前被静默丢弃、run 起来什么也不做，现在 422 | 改正键名 |
| `interrupt_before`/`interrupt_after` 由 `str \| list[str]` 收为 `"*" \| list[str]` | 裸节点名（`interrupt_before="tools"`）现在 422 | 写成列表 `["tools"]`；`"*"` 通配符不变 |
| cron `metadata` 按 run 的规则校验 | 嵌套值、非法键、超过 31 个键的 cron 现在创建时 422（此前能建、首次触发才炸）；`cron_id` 为保留键 | 拍平为 `str`/`int`/`float`/`bool` |
| cron `metadata` 下传到触发的 run | 触发的 run 的 `metadata` 从 `{}` 变为 cron metadata + `cron_id` | 无；按 schedule 过滤 run 现在可用 |
| `assistant_id`/`run_id`/`cron_id` 创建时可选 | 追加字段，省略即由服务端生成；显式传值时重复返回 409 | 无 |
| `config.configurable.assistant_id` 由服务端写入 | 与 `thread_id`/`run_id` 同为服务端权威值，请求体里的同名键被覆盖 | 依赖该键传自定义值的图改用其他键名 |

## 七、错误响应契约

SDK 的 `_map_status_error()` 按状态码映射异常类（400→`BadRequestError`、401→`AuthenticationError`、403→`PermissionDeniedError`、404→`NotFoundError`、409→`ConflictError`、422→`UnprocessableEntityError`、429→`RateLimitError`、≥500→`InternalServerError`）。项目的状态码使用与之一致。

错误消息由 `_extract_error_message()` 从响应体按 `message` → `detail` → `error` 顺序提取，**且要求值是非空字符串**。所有状态码（含 422）现在都走 `{error, message, details}` 信封，`message` 恒为字符串。

`APIError` 另会 best-effort 读 `code`、`param`、`type`；项目的信封用 `error`/`details` 命名，故 `err.code`、`err.type` 恒为 `None` —— 不影响异常类型判断。

`err.request_id` 读 `x-request-id` 响应头，项目以默认配置挂载 `CorrelationIdMiddleware` 暴露 `X-Request-ID`，httpx 查找大小写不敏感，已对齐。

## 八、认证

SDK 从 `api_key` 参数或 `LANGSMITH_API_KEY` / `LANGCHAIN_API_KEY` 环境变量解析出 key，以 **`x-api-key`** 请求头发送。

项目不硬编码任何认证头，而是把认证委托给 `aegra.json` 的 `auth.path` 所指的 `@auth.authenticate` handler。因此对齐是**可配置的而非默认的**：要让 SDK 客户端开箱可用，自定义 handler 必须读取 `x-api-key` 头。见 `docs/guides/authentication`。

## 九、流式协议

### 9.1 run 级 `StreamMode`

九个取值（`values`、`updates`、`messages`、`messages-tuple`、`custom`、`debug`、`events`、`tasks`、`checkpoints`）全部为合法输入，由 `models/enums.py:StreamMode` 校验 —— 非法值 422 而非静默产出空流。`tasks`/`checkpoints` 透传 LangGraph，无专门事件构造。

`langgraph` 核心库自己的 `StreamMode` 只有 7 个值（无 `events`、`messages-tuple`）—— 这两个是 API 层概念，由 Agent Server 翻译成核心库的模式，项目已实现该翻译（`messages-tuple` 对 Python graph 归一化为 `messages`）。

### 9.2 thread 级 `ThreadStreamMode`

| 模式 | 语义 | 状态 |
|:--|:--|:-:|
| `run_modes` | 转发该 thread 上各 run 的 run 级事件 | ✅ |
| `lifecycle` | thread 生命周期事件（`run.start`/`run.end`） | ✅ |
| `state_update` | 每个 run 结束后的 thread 状态快照 | ✅ |

省略时默认 `run_modes`，与 SDK 客户端默认一致。

### 9.3 v1 / v2 事件形状

`runs.stream()` 的 `version` 参数**不发给服务端** —— 它是纯客户端转换（`_wrap_stream_v2` → `_sse_to_v2_dict`）。但转换依赖服务端的事件名约定：

```
event.split("|")  →  parts[0] 作为 type，parts[1:] 作为 ns
```

`graph_streaming.py` 在 `stream_subgraphs=True` 时拼 `f"{mode}|{ns_str}"`，因此 v1 与 v2 均可用。

> **勿与项目的 `event_streaming_v2` 混淆。** 后者是 Aegra 自有的双向命令协议，服务于 `POST /threads/{id}/commands` 与 `POST /threads/{id}/stream/events` 两个非 SDK 端点，与 SDK 的 v2 流式格式无关。

### 9.4 断线重放

| 能力 | 状态 |
|:--|:-:|
| `Last-Event-ID` 头恢复 | ✅ |
| `"-"` 从头重放 | ⚠️ 未显式识别该值，但 `replay()` 找不到匹配 id 时 fallback 返回全部事件，效果等价 |
| `stream_resumable` | ⚠️ 事件缓冲在内存，不跨重启（见 4.4 末） |
| thread 级 join | ✅ |

## 十、验证方式

本文的「已实现」不是靠读代码断言的，每一项都跑过。复现步骤：

**静态**

```bash
make lint          # ruff check
make security      # bandit，无 issue
make openapi       # 重新导出 spec，diff 应为空
uv run --package aegra-api pytest libs/aegra-api/tests/unit libs/aegra-api/tests/integration
```

单元 + 集成 1840 passed / 1 skipped。与本次对齐直接相关的新增用例：

| 文件 | 覆盖 |
|:--|:--|
| `tests/unit/test_services/test_multitask.py` | 四种策略 + `rollback` 的 501 门控在中断任何 run 之前触发 |
| `tests/unit/test_services/test_thread_state_cache.py` | 中断编码成 SDK 的 `{value, id}`、指纹与键序无关、缓存写失败不外抛 |
| `tests/unit/test_services/test_webhooks.py` | outbox 行不自行 commit、退避封顶、达上限置 `failed` |
| `tests/unit/test_models/test_run_create_contract.py` | `checkpoint_id` 折叠、`checkpoint_during` → `durability`、webhook URL 校验、各枚举非法值 422 |
| `tests/unit/test_api/test_validation_envelope.py` | 422 信封，含自定义 app 合并的回归 |
| `tests/unit/test_core/test_query.py` | 排序方向/tie-break、`select` 投影键名、`extract` 路径解析、分页游标 |
| `tests/integration/test_api/test_bulk_runs.py` | `POST /runs/cancel` 四种目标组合、`POST /runs/batch` |

**迁移**

在真实 Postgres 上从 `d1f7b3a9c5e2` 升到 `a3d6e0b95f17` 跑通。另外单独验证了四件容易想当然的事：

- DDL 可重入（重跑只出 `NOTICE ... skipping`）。
- 队列认领的 `UPDATE ... WHERE run_id = (SELECT ... FOR UPDATE SKIP LOCKED)` 确实只取最旧一条、只影响一行。
- 状态缓存的 upsert 在 `values_hash` 未变时是 `INSERT 0 0`，即真的跳过写入。
- `thread_state` 的 `values` 是 SQL 保留字，SQLAlchemy 编译出的是**不带引号**的 `values`；已确认 Postgres 在 INSERT 列名与 `SET` 位置接受该写法。

**端到端**

对真实服务（`docker compose up -d`，migrations 走启动路径）跑 37 项契约检查，覆盖本文第三节的每一处 ⚠️ 与新增字段：36 通过。唯一未通过的是 4.4 记录的 `acopy_thread` 限制 —— 即预期行为。

## 十一、如何重做这个分析

升级 `langgraph-sdk` 后，解析 `.venv/Lib/site-packages/langgraph_sdk/_async/{assistants,runs,threads,cron,store}.py`（顶层 `LangGraphClient` 只组装这五个子客户端；`_sync` 与 `_async` 端点集合已验证一致），找出所有 `self.http.<verb>(...)` 调用；项目侧先 `make openapi` 再解析 `docs/openapi.json`。两边取差集。

以下五个坑是实际踩过的，直接按朴素方式提取会得出错误结论：

1. **`@overload` 方法要取最后一个同名定义。** `assistants.search`、`threads.update`、`runs.create/stream/wait` 都有重载声明，声明体是 `...`，提取出来是空集。
2. **路径可能是三元表达式或变量。** `create()`/`stream()`/`wait()` 的路径形如 `f"/threads/{id}/runs" if thread_id else "/runs"`，必须展开成两条；`stream()`/`wait()` 还先赋值给 `endpoint` 变量再传入。不展开会少算 5 个端点。
3. **动词不能按方法名推断。** `join()`/`join_stream()` 走 `http.request_reconnect`/`http.stream`，真实动词在 `method=` 实参里。
4. **嵌套键会混进请求字段。** `supersteps` 内部的 `updates`/`values`/`as_node`/`command` 和 `ttl` 内部的 `strategy` 都是嵌套结构的键。
5. **有些「缺失」其实可用，有些「参数」根本不发给服务端。** `graph_id` 被 SDK 合并进 `metadata`；`response_format` 是纯客户端参数，服务端只需返回 `X-Pagination-Next` 头；`version` 同理。

判定时端点存在性可以机械比对，但**参数是否真正生效必须读实现** —— 旧版的 `_stream_mode` 就是签名里声明了、函数体从未使用的例子，只看 OpenAPI 会误判为「已支持」。
