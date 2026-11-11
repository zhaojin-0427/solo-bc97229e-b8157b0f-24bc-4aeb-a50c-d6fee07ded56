# Scan Time Normalizer — 航空货运扫描时间归一化 API

将承运商导出的**本地扫描时间**按 IANA 时区规则归一化为 **UTC**，并在夏令时回拨（本地时间重复）
/ 拨快（本地时间不存在）场景下保持转运顺序语义正确：

- 每条扫描记录的本地时间对应 **0、1 或 2 个合法 UTC 候选**；
- 航段/转运衔接约束两条记录之间的 UTC 耗时落在闭区间 `[min_seconds, max_seconds]`，
  衔接可共用记录，且**全部衔接必须同时满足**；
- 服务对每条记录返回**能出现在至少一套全局可行解释中的全部候选**：
  - 仅剩一个时标 → `determined`（确定），直接给出 `utc`；
  - 仍有多个 → `needs_review`（待核对），返回全部候选，**不擅自选取**；
- 任一记录无合法候选，或衔接整体无解 → **HTTP 409** 并明确原因，**不返回部分转换**。

## 并发隔离与容量控制

入仓流水线会并发提交承运商扫描批次，`/normalize` 的计算按下列规则隔离与限流：

| 规则 | 行为 |
| --- | --- |
| **进程隔离** | 每个通过校验的批次都在一个**独立、短命的 OS 进程**中计算（`multiprocessing` spawn）。API 进程只做监督，一批崩溃/超时不会中止另一批。 |
| **容量上限** | 单个 API 进程最多同时计算 **2 批**（`MAX_CONCURRENT_BATCHES`）。通过校验的请求在名额占满时**立即返回 `429`，不排队**。 |
| **批次时限** | 每批有可配置的挂钟时限，默认 **10 秒**（`BATCH_TIMEOUT_SECONDS`）。超时 → 终止并回收计算进程后返回 **`504`**；**迟到结果一律丢弃，不作为成功返回**。 |
| **进程异常** | 计算进程非零退出或内部异常 → 返回 **`503`**。 |
| **名额回收** | 只有在计算进程**真正结束并被 `wait` 回收之后**才释放名额；超时/崩溃都不会泄漏名额，后续请求可立即复用。 |
| **健康检查** | 阻塞式进程监督放在线程中执行，事件循环不被占用；计算期间 `GET /healthz` 始终可响应，并回报在算批次数。 |
| **整批语义** | 每批要么完整成功（200），要么整体失败（409/422/429/503/504），**从不返回部分转换**，结果可供下游整批入仓。 |

### 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `MAX_CONCURRENT_BATCHES` | `2` | 单 API 进程同时计算的批次数上限；占满后新请求立即 429 |
| `BATCH_TIMEOUT_SECONDS` | `10` | 单批挂钟时限（秒）；超时杀进程并返回 504 |
| `SLOW_BATCH_ID` | 空（关闭） | 演示开关：含该记录 id 的批次，计算进程持续运行超过时限（用于 429/504 演示） |
| `CRASH_BATCH_ID` | 空（关闭） | 演示开关：含该记录 id 的批次，计算进程以非零码硬退出（用于 503 演示） |

> 两个演示开关只用于在交互式文档中制造过载/超时/崩溃场景，**请求与响应结构本身不变**；
> 不配置时它们完全不参与计算。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

compose 配置把时限调成 2 秒并打开演示开关，便于在文档里快速看到过载与超时效果。
服务启动后：

- 交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
  —— 内置五个可一键试用的示例（夏令时回拨消歧 / 不存在本地时间 409 / 衔接冲突 409 /
  慢批次 504·429 / 崩溃批次 503）；
- 健康检查：`GET http://localhost:8000/healthz`；
- 归一化接口：`POST http://localhost:8000/normalize`。

### 在 Swagger UI 中演示归一化与过载响应

1. **正常归一化（200）**：选择 `dst_fallback_resolved` → Try it out → Execute，返回 5 条记录的
   完整 UTC 归一化（4 条 `determined`、1 条 `needs_review`）。
2. **占满两个计算名额**：连开两个浏览器标签（或两个 curl），各执行一次
   `slow_batch_timeout_or_overload`。两次请求都保持在算状态。
3. **立即过载（429，不排队）**：在两个慢批次运行期间，于任一标签再执行一次任意示例
   （例如正常的 `dst_fallback_resolved`），立刻得到 `429 capacity_exhausted`，
   响应体说明未排队且 `max_concurrent_batches=2`。
4. **健康检查不受影响**：同一时间访问 `/healthz`，立即返回
   `{"status":"up","batches_in_progress":2}`。
5. **超时回收（504）**：约 2 秒后两个慢批次先后返回 `504 batch_timeout`；再看 `/healthz`，
   `batches_in_progress` 回到 0。
6. **名额复用**：504 之后再次执行 `dst_fallback_resolved`，正常返回 200——名额已被回收复用。
7. **计算进程异常（503）**：执行 `crashing_batch_503`，计算进程硬退出，返回
   `503 computation_unavailable`（含退出码）；随后再发正常请求仍是 200，服务不受影响。

等价的命令行演示：

```bash
# 两个慢批次占满名额（后台各跑一个）
curl -s -X POST localhost:8000/normalize -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"SLOW","local_time":"2024-06-01T10:00:00","timezone":"UTC"}],"links":[]}' &
curl -s -X POST localhost:8000/normalize -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"SLOW","local_time":"2024-06-01T10:00:00","timezone":"UTC"}],"links":[]}' &
# 第三个请求立即 429；期间 healthz 正常；慢批次约 2s 后 504
curl -s localhost:8000/healthz
# 计算进程硬退出 -> 503
curl -s -X POST localhost:8000/normalize -H 'Content-Type: application/json' \
  -d '{"records":[{"id":"CRASH","local_time":"2024-06-01T10:00:00","timezone":"UTC"}],"links":[]}'
```

## 接口说明

### `POST /normalize`

**请求体**

| 字段 | 说明 |
| --- | --- |
| `records` | 1–200 条扫描记录：`id`（唯一）、`local_time`（精确到秒的朴素本地时间，不得带偏移）、`timezone`（IANA 时区名） |
| `links` | 0–400 条衔接：`from_id` / `to_id`（须为已声明记录）、`min_seconds` / `max_seconds`（非负整数秒，且 min ≤ max） |

**响应一览**

| 状态码 | 场景 | 要点 |
| --- | --- | --- |
| `200` | 完整归一化成功 | `determined` 给唯一 `utc`；`needs_review` 给全部候选、`utc=null` |
| `409` | 无合法候选 / 衔接整体无解 | 明确原因与不可约冲突子集，**无部分转换** |
| `422` | 请求体不合法 | 未知时区、重复 ID、引用未声明记录、min > max、超 200/400 上限等；**校验先于名额分配，不占计算名额** |
| `429` | 两个计算名额均忙 | 立即返回、不排队，`max_concurrent_batches` 明示上限 |
| `504` | 批次超过时限 | 进程已终止并回收，迟到结果丢弃，无部分转换 |
| `503` | 计算进程异常退出 | 含失败原因与退出码；进程已回收、名额已释放 |

**409 响应**（不返回任何部分转换）：

- `no_legal_candidate` — 本地时间在时区规则下不存在（落入拨快跳空），`invalid_records` 列出涉事记录；
- `no_global_solution` — 衔接互相矛盾、整体无解，`conflicting_links` 给出**不可约冲突子集**
  （再删掉任意一条衔接即可恢复有解），`involved_records` 列出涉事记录。

### 演示：夏令时回拨消歧

2024-11-03 美国冬令时回拨，纽约 `01:30` 出现两次（05:30Z / 06:30Z）：

```bash
curl -s -X POST http://localhost:8000/normalize \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"id": "ARR_JFK", "local_time": "2024-11-03T01:30:00", "timezone": "America/New_York"},
      {"id": "DEP_JFK", "local_time": "2024-11-03T02:45:00", "timezone": "America/New_York"},
      {"id": "ARR_ORD", "local_time": "2024-11-03T04:10:00", "timezone": "America/Chicago"},
      {"id": "DEP_LAX", "local_time": "2024-11-03T03:20:00", "timezone": "America/Los_Angeles"},
      {"id": "XFR_ORD", "local_time": "2024-11-03T01:30:00", "timezone": "America/Chicago"}
    ],
    "links": [
      {"from_id": "ARR_JFK", "to_id": "DEP_JFK", "min_seconds": 3600, "max_seconds": 7200},
      {"from_id": "DEP_JFK", "to_id": "ARR_ORD", "min_seconds": 7200, "max_seconds": 10800},
      {"from_id": "ARR_ORD", "to_id": "DEP_LAX", "min_seconds": 1800, "max_seconds": 7200},
      {"from_id": "ARR_JFK", "to_id": "XFR_ORD", "min_seconds": 0, "max_seconds": 86400}
    ]
  }'
```

实际返回（节选）：

```json
{
  "status": "ok",
  "summary": {"total": 5, "determined": 4, "needs_review": 1},
  "records": [
    {"id": "ARR_JFK", "status": "determined", "utc": "2024-11-03T06:30:00Z", "candidates": ["2024-11-03T06:30:00Z"], "...": "..."},
    {"id": "XFR_ORD", "status": "needs_review", "utc": null, "candidates": ["2024-11-03T06:30:00Z", "2024-11-03T07:30:00Z"], "...": "..."}
  ]
}
```

`ARR_JFK` 的两个回拨候选中，`05:30Z` 会使到 `DEP_JFK`（07:45Z）的地面耗时达 2h15m、
超出 1–2h 窗口，被全局约束排除，故唯一确定为 `06:30Z`；`XFR_ORD` 的两个候选都满足全部
衔接，保持 `needs_review`，交由人工核对。

## 实现原理

1. **候选生成**（`app/solver.py: utc_candidates`）：对本地时间分别以 `fold=0/1` 附着时区后转 UTC，
   再回读校验——回拨重复得到 2 个候选，拨快跳空得到 0 个，常规时间 1 个。
2. **全局可行性 = 2-SAT**：每条含 2 个候选的记录对应一个布尔变量；每条衔接把"越界的候选组合"
   翻译成子句（禁止该组合）。2-SAT 可用蕴含图 + 强连通分量在线性时间内判定。
3. **逐候选过滤**：对每个候选施加单元子句假设后重判可满足性，保留下来的即为
   "能出现在至少一套全局可行解释中"的候选——精确求解，非启发式。
4. **冲突定位**：整体无解时，对衔接做贪心删除得到不可约冲突子集，作为 409 的明确原因。
5. **进程隔离监督**（`app/runner.py`）：每批 `spawn` 一个独立进程，父进程以挂钟时限轮询结果队列
   与进程存活状态；超时先 `SIGTERM` 再 `SIGKILL` 升级，并在 `join` 回收后才返回 `504`；
   进程无结果退出立即判 `503`；队列中任何迟到消息都被显式排空丢弃。
6. **容量闸门**（`app/limits.py`）：基于锁的非排队信号量，`try_acquire` 失败即 429；
   名额释放写在路由 `finally` 中，且只发生在监督逻辑回收进程之后。

规模上限（200 记录 / 400 衔接）下，纯计算为毫秒级；时限与并发仅用于约束异常/过载批次。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt   # 含 pytest / httpx
uvicorn app.main:app --reload          # 本地起服务
python -m pytest tests/ -q             # 31 个用例
```

测试分两层：

- `tests/test_api.py`（19 个）：消歧/传播/冲突/校验/上限等原有语义，全部经真实子进程路径验证；
- `tests/test_concurrency.py`（12 个）：真实 spawn 进程下的 429 不排队、504 超时杀进程与名额回收、
  503 硬退出/内部异常、一批崩溃不影响另一批、计算期间 healthz 可响应、422 不占名额、
  迟到结果丢弃、无进程泄漏等。

## 目录结构

```
app/
  main.py      # FastAPI 入口、路由、409/429/503/504 错误映射、Swagger 示例
  schemas.py   # 请求/响应模型与输入校验（时区、精度、引用完整性、上限）
  solver.py    # UTC 候选生成 + 2-SAT 全局可行性 + 不可约冲突子集（在子进程内执行）
  runner.py    # 每批一个 spawn 进程：时限轮询、终止回收、超时/异常/迟到结果处理
  limits.py    # 非排队容量信号量（默认 2 名额）
  settings.py  # 环境变量配置（并发上限、时限、演示开关）
tests/
  test_api.py          # 原有领域语义（19）
  test_concurrency.py  # 隔离/容量/时限/健康检查（12）
  worker_doubles.py    # 可 pickle 的测试用 worker（慢/崩溃/抛异常）
Dockerfile / docker-compose.yml / requirements.txt / requirements-dev.txt
```
