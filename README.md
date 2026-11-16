# 电表日结 SQL 核对 API

数仓工程师改写电表日结的关联 SQL 后，用本服务确认读数**没有漏计或重复计入**。
提交两条只读 `SELECT`，服务在同一已提交快照上并行读取两侧结果，按
**行内容 + 出现次数**做多重集核对（忽略行序，`NULL` 与空串视为不同），
返回一致结论或最多 20 种差异行及两侧次数。

## 1. 快速启动

```bash
docker compose up --build
# API:   http://localhost:8000
# 文档:  http://localhost:8000/docs   （Swagger UI，可直接发请求演示）
```

启动后 PostgreSQL 自动执行 `sql/init.sql`，建好 `meters` / `readings`
源表并写入示例数据。

**`/docs` 演示入口**：展开 `演示` 分组，直接执行 `POST /jobs/demo`，
即用内置的两条等价示例查询发起一轮核对；随后用 `GET /jobs/{job_id}`
查看结论，或执行 `POST /jobs/{job_id}/cancel` 演示取消语义。
`GET /demo/queries` 会返回这两条 SQL，便于复制到 `POST /jobs` 自行改写提交。

停止并清理：

```bash
docker compose down -v
```

## 2. 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 存活探针，不触碰数据库 |
| `POST` | `/jobs` | 提交核对任务（同时只允许一组运行，占用中返回 `409`） |
| `GET`  | `/jobs` | 列出近期任务（含终态），状态查询始终可用 |
| `GET`  | `/jobs/{job_id}` | 查询单个任务状态与结论 |
| `POST` | `/jobs/{job_id}/cancel` | 请求取消，及时受理；终态已发布时返回 `cancel_accepted=false` |
| `POST` | `/jobs/demo` | 用内置示例 SQL 发起一轮核对（`/docs` 一键演示） |
| `GET`  | `/demo/queries` | 返回内置示例的两条 SQL |

任务状态：`pending → running →（cancelling）→ succeeded | failed | cancelled | timeout`。
失败、取消、超时一律 **不发布比较结论**（`result` 为 `null`），仅给出原因。

### 收尾语义（本次修复重点）

- **任一侧失败立即停止另一侧**：发现失败后马上对该侧发起 SQL 取消
  （必要时 `pg_terminate_backend`），不等待另一侧自然跑完；
- **保留原始失败原因**：失败先到则终态固定为 `failed` 并保留该侧原始异常，
  随后到达的超时或取消不会覆盖它；
- **取消与成功竞态**：取消先被受理则不发布结论（`cancelled`，`result=null`）；
  成功先发布则保持 `succeeded` 与 `result`，取消接口返回
  `cancel_accepted=false` 明确告知不可取消；
- **状态与结论同次发布**：二者在同一临界区内写入，查询永远只会看到
  “运行中且无结论”或“终态 + 对应结论”，非成功不暴露任何结论；
- **先收尾后报终态**：全部 SQL 与读取线程退出、连接关闭、暂存清空之后
  才报终态并释放名额；收尾较慢时状态查询与取消受理仍即时返回。

任务进入终态后，状态查询接口仍可继续使用；`GET /jobs` 只保留近期 20 条。

### 请求示例

```bash
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "left_sql":  "SELECT meter_id::bigint, wh::bigint FROM readings",
  "right_sql": "SELECT meter_id::bigint, wh::bigint FROM readings",
  "name": "daily-settlement-v2"
}'
```

### 结论结构（成功时）

```json
{
  "status": "succeeded",
  "result": {
    "identical": false,
    "columns": ["meter_id", "wh"],
    "column_types": ["bigint", "bigint"],
    "left_row_total": 504013,
    "right_row_total": 504012,
    "matched_group_count": 504011,
    "diff_group_total": 1,
    "diffs_shown": 1,
    "diffs": [
      {"values": [1001, 1150], "left_count": 2, "right_count": 1}
    ]
  }
}
```

## 3. 对提交 SQL 的约束

- 必须是**单条只读 SELECT**（允许 `WITH` / `UNION` / `INTERSECT` / `EXCEPT`），
  静态解析拒绝多语句与任何写操作 / DDL / `SELECT INTO` / `LOCK` / `CALL` 等；
- 结果列只允许 `bigint` 与 `text`，两侧**列数相同、对应位置类型相同**，
  否则本轮失败且不给出比较结论；
- 数据库连接再叠加一层纵深防御：`READ ONLY` 事务、`statement_timeout`、
  `lock_timeout`，即使静态校验被绕过也无法写入或长时间持锁。

## 4. 关键设计

### 同一已提交快照，源表仍可更正

每条任务开三条独立连接：快照连接执行
`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` + `pg_export_snapshot()`，
两条读取连接分别 `SET TRANSACTION SNAPSHOT` 绑定**同一个快照**。
核对期间其他会话对 `readings` / `meters` 的更正照常提交，但对本轮不可见。

### 内存有界的外部归并（不全量载入、不靠哈希判等）

1. 命名服务端游标按 `itersize` 分批拉取，客户端从不缓存整表；
2. 每 `SORT_CHUNK_ROWS`（默认 2 万）行在进程内排序，逐行流式 pickle 落盘为一个 run；
3. 归并阶段对两侧 run 做单轮 k 路堆归并（堆中每流只驻留一行），
   键随行走，按全序键聚合等值组后比较两侧次数；
4. 差异明细最多保留 20 种，但总行数、差异组总数等计数覆盖全量数据；
5. 全程不使用整表哈希作为判等依据。

### 单名额、30 秒时限与终止语义

- 进程内单名额信号量：全局同时只运行一组，冲突返回 `409`；
- 看门狗按 `JOB_TIMEOUT_SECONDS`（默认 30）判定超时；
- 失败/取消/超时通过同一个 `abort_event` 联动：任一侧失败会立即
  `conn.cancel()` 中止另一侧与全部本轮 SQL，必要时 `pg_terminate_backend`；
- 终止原因按事件先后裁决：真实失败 > 取消 > 超时，先到者决定终态与原因；
- 严格按顺序收尾：**中止并等待读取线程退出 → 关闭连接 → 清空暂存目录 →
  释放名额并原子发布终态**，因此状态查询一旦看到终态，名额必然已经释放、
  暂存必然已经清空；状态与 `result` 在同一次发布中写入，不会被观察到撕裂。

## 5. 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PG_DSN` | `postgresql://recon:recon@postgres:5432/recon` | 数据库连接串 |
| `JOB_TIMEOUT_SECONDS` | `30` | 单轮硬时限 |
| `PG_STATEMENT_TIMEOUT_MS` | `28000` | 服务端单语句超时（留 2s 收尾余量） |
| `SORT_CHUNK_ROWS` | `20000` | 单个排序块行数 |
| `MAX_DIFF_TYPES` | `20` | 差异展示上限 |
| `WORK_DIR` | `/tmp/recon-work` | 外部排序暂存目录（独立卷） |

## 6. 本地自测

```bash
# 单元 + 集成测试（集成测试需要一个 PostgreSQL；可用 PG_DSN 指定）
python -m pytest tests/ -q

# 可选：仅跑可控交错的收尾语义回归
#   test_lifecycle.py    不依赖数据库，验证失败/取消/成功的交错裁决与原子发布
#   test_lifecycle_pg.py 真实 PostgreSQL 下验证取消先受理、成功先发布
python -m pytest tests/test_lifecycle.py tests/test_lifecycle_pg.py -q

# 50 万行级端到端压测（内存峰值、暂存占用、结论正确性、清理与名额释放）
python scripts/load_test.py
```

本次交付实测（受限沙箱、嵌入式 PostgreSQL，两侧各 336,012 行、
含一次重复计入差异）：端到端约 12.7 秒，进程峰值 RSS 约 68 MiB，
暂存峰值约 28.8 MiB，结束后暂存目录清空、名额释放，全部校验通过。
容器内 50 万行级的吞吐与内存表现以同一脚本复测为准。

> 沙箱说明：本环境的 seccomp 屏蔽了 PostgreSQL 的查询取消信号，
> `conn.cancel()` / `pg_terminate_backend` 无法中断已在执行的 SQL；
> 因此集成测试用“只置标志的取消 + 逐批延迟”构造可控交错。
> 正常 Linux/容器内核下这两条取消路径均生效。

## 7. 目录结构

```
app/
  config.py          运行期配置
  sqlvalidation.py   只读 SELECT 静态校验
  db.py              快照会话、只读连接、命名游标流式拉取、列类型闸门
  external_merge.py  流式 run、k 路归并、多重集核对
  manager.py         单名额编排、看门狗、取消/超时/失败的事件裁决与统一收尾
  demo.py            /docs 演示用示例 SQL
  models.py          API 模型
  main.py            FastAPI 路由
sql/init.sql         源表、示例数据、示例查询
scripts/             压测数据生成与负载脚本
tests/               单元与集成测试
docker-compose.yml   应用（128MiB 上限）+ PostgreSQL
Dockerfile
```
