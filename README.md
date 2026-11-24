# 灰度放量核查 API（FastAPI + Prometheus 直方图）

灰度发布后，值班员需要确认最近的请求里是否**至少 99% 在延迟限时内完成**。
本服务周期性抓取两个本地样例目标暴露的 Prometheus 延迟直方图，按桶计数增量
算出两实例合计达标比例的**上下界**，并给出放量建议。

## 判定规则

每实例取**最近两次抓取**，且必须同时满足：

1. 两次抓取都在近 **3 秒**新鲜窗口内；
2. 两次抓取来自**同一进程**（`run_id` / 进程启动时间一致，重启会被识别）；
3. 两次抓取间隔**至少 1 秒**。

满足后用两次抓取的**累计桶计数之差**得到窗口内各桶的增量，再按给定延迟阈值
`threshold` 计算达标请求数的上下界：

- 下界：确定落在 `<= threshold` 桶内的请求数；
- 上界：可能落在 `<= threshold` 内的请求数；
- 当阈值恰好等于桶边界时答案确定（上下界相等）；当阈值落在某个桶内部时，
  该桶内的请求无法逐个判断，**保留不确定范围**。

两实例桶边界不同也没关系：各自算出上下界后再相加。

| 条件 | 决策 |
| --- | --- |
| 达标比例**下界 ≥ 99%** | `proceed` 可以放量 |
| 达标比例**上界 < 99%** | `halt` 停止放量 |
| 区间跨越 99%，或两实例窗口合计**零流量** | `inconclusive` 证据不足 |
| 等待样本超时 | `inconclusive`，并在 `missing_instances` 中点名缺证据的实例及原因 |

## 接口

打开 `http://localhost:8000/docs` 可直接在 Swagger UI 填参调用。

### `GET /check`

查询参数：

| 参数 | 含义 |
| --- | --- |
| `threshold` | 延迟阈值（秒，> 0），超过视为不达标 |
| `max-wait` | 最长等待样本时间（秒，0 < x ≤ 60），超时即按缺证据返回 |
| `target` | 可选，达标比例目标，默认 `0.99` |

样本齐了立即返回；缺样本时等待到 `max-wait`。每个请求独立计时，
一个请求超时不会影响其他并发核查；所有并发核查共用同一套抓取样本。

### `GET /stats`

返回每个实例的实际抓取次数（成功/失败）、最近抓取时刻、当前进程 id，
以及当前被选为样本的两次抓取时刻、间隔与窗口请求数。

### `GET /health`

存活探针。

## 样例目标的控制接口

两个样例目标（`sample-a`、`sample-b`）桶边界不同：

- A：`0.05, 0.1, 0.25, 0.5, 1, 2.5, 5`
- B：`0.1, 0.3, 0.7, 1.5, 3, 6`

| 接口 | 作用 |
| --- | --- |
| `POST /inject` | body `{"duration_seconds": 0.23, "count": 1}`，注入请求耗时 |
| `POST /restart` | 模拟进程重启：新 `run_id`、新启动时间、计数清零 |
| `GET /state` | 查看内存中的桶计数与进程信息 |
| `GET /metrics` | Prometheus 文本格式直方图（始终是整表，Prometheus 自身抓取此端点） |
| `GET /metrics_delta?since_seq=N` | JSON 增量 exposition（见下节） |

## 增量抓取（降低样例抓取流量）

桶边界很多时，每次抓取都下载整张延迟直方图浪费带宽。核查器对两个**样例目标**
改用 `/metrics_delta` 增量协议：

- **首次抓取或样例进程重启**：返回 `type="full"` 整表（全部互斥桶、尾桶、
  `count`/`sum`、`run_id`、游标 `seq` 等元数据），核查器据此建立会话基准；
- **其余成功抓取**：带上次成功处理响应的游标 `since_seq`，目标只返回自基准以来
  **有新增请求的互斥延迟桶（含 `+Inf` 尾桶）** 的增量，以及绝对 `count`/`sum`
  等必要元数据；目标端用“桶 → 最近变更 seq”的脏标记生成响应，工作量只随
  **变动桶数**增长，与总桶数无关；
- **零增量也是成功抓取**：`changed_buckets` 为空数组，照常推进新鲜窗口；
- **响应丢失或处理失败时基准不动**：核查器保留上次成功接收的会话，同一进程的
  下次成功抓取仍以旧游标请求，目标把整个缺失区间的计数一次性补齐；
- 样例**重启**后游标与 `run_id` 复位，旧游标不再可识别，目标自动回整表，
  核查器以新进程重建会话；样本资格规则（同进程、新鲜窗口、1 秒间隔）不变；
- 核查器把互斥桶增量重建成与全量抓取**逐值相同的累计桶计数**，因此相同采样下
  `/check` 的请求数、达标上下界和放量结论与全量抓取完全一致（下游判定逻辑未改动）；
- **普通 Prometheus 目标**没有 `/metrics_delta`（返回 404），核查器探测一次后
  永久回退到 `/metrics` 文本全量抓取，行为与原先一致。

`/metrics_delta` 的 JSON 形态：

```jsonc
// 整表（首次 / 重启 / 未知或未来游标）
{"type":"full","run_id":"ab12…","seq":3,"boundaries":[0.1,0.5,1.0],
 "exclusive_buckets":[7,11,4],"tail":2,"count":24,"sum":12.0,"scrape_seq":5}
// 增量（仅变动互斥桶，le="+Inf" 为尾桶；空 changed_buckets 即零增量成功）
{"type":"delta","run_id":"ab12…","since_seq":3,"seq":5,
 "changed_buckets":[{"le":0.5,"count":3},{"le":"+Inf","count":1}],
 "count":28,"sum":13.4,"scrape_seq":7}
```

### `/stats` 里的流量与会话字段

每个实例除原有的抓取次数外，还报告：

| 字段 | 含义 |
| --- | --- |
| `transport_mode` | `incremental-json`（样例增量）/ `full-table-only`（普通目标回退）/ `unknown` |
| `full_table_body_bytes` | 整表响应正文**累计接收字节数**（文本整表与 JSON 整表都计入） |
| `delta_body_bytes` | 增量响应正文**累计接收字节数** |
| `full_table_responses` / `delta_responses` | 两类成功响应的次数 |
| `delta_session` | 当前会话：`run_id`、游标 `seq`、桶边界、重建出的互斥桶计数、`count`/`sum` |

在 `http://localhost:8000/docs` 可直接调用 `/stats` 查看每个样例的两类累计字节数。

## 用 docker compose 启动

```bash
docker compose up --build
# verifier  http://localhost:8000/docs
# sample-a  http://localhost:9101/metrics
# sample-b  http://localhost:9102/metrics
```

试跑示例：

```bash
# 每实例注入 99 个 0.2s + 1 个 2s 请求
curl -X POST localhost:9101/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":0.2,"count":99}'
curl -X POST localhost:9101/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":2.0,"count":1}'
curl -X POST localhost:9102/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":0.2,"count":99}'
curl -X POST localhost:9102/inject -H 'content-type: application/json' \
  -d '{"duration_seconds":2.0,"count":1}'

# 等 ~2 秒让下一个抓取窗口覆盖注入流量，然后核查
curl 'localhost:8000/check?threshold=0.5&max-wait=8'
# 模拟进程重启
curl -X POST localhost:9101/restart
```

> 注意：核查只看“最近两次抓取之间”的增量，因此注入后要等下一次抓取
> （抓取间隔约 1.05s），流量才会出现在样本窗口里；再下一次抓取后窗口即滑过。

## 不用 Docker 的本地启动

```bash
pip install -r requirements.txt
./run_local.sh
```

## 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

- `tests/test_histogram.py`：解析、桶增量、上下界、双实例不同桶布局的纯函数测试；
- `tests/test_delta_protocol.py`：JSON 增量协议的整表解析、增量合并、丢响应补齐、
  重启/回滚校验与“重建快照等价于整表”测试；
- `tests/test_sample_target_delta.py`：样例目标的游标/基准推进、零增量、丢失补齐、
  重启回整表、O(变动桶) 负载与 HTTP 契约测试；
- `tests/test_scraper_delta.py`：核查器侧首抓整表→增量、失败保留基准与下次补齐、
  重启重建会话、普通目标 404 回退全量、字节计数与会话共享测试；
- `tests/test_scraper_selection.py`：新鲜窗口、1 秒间隔、跨进程/计数器回滚的样本选择；
- `tests/test_integration.py`：真实 uvicorn 进程的端到端测试，覆盖
  proceed / halt / 不确定区间 / 零流量 / 重启恢复 / 缺实例超时 / 并发共享样本 /
  统计，以及增量流量节省、丢响应补齐、重启整表与全量等价性。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SCRAPE_TARGETS` | `sample-a=http://sample-a:9101/metrics,sample-b=...` | 抓取实例，`名称=URL` 逗号分隔 |
| `SAMPLE_BUCKETS` | A/B 各自默认 | 样例目标的桶边界，逗号分隔 |
| `INSTANCE_ID` | `sample-a` | 样例目标实例标识 |
