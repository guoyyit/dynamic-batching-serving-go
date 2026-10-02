# 配置项与观测指标

## 1. GlobalConfig

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
 `TickInterval` | duration | `2ms` | 扫描分辨率实际上取 `min(TickInterval, 1ms)`；深度触发由提交/回队立即唤醒，不等待节拍。 |
| `NumWorkers` | int | `2` | 消费批次的固定工作协程数，同时决定批次通道容量；负载再高也不新增协程。 |

## 2. ModelConfig

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `Name` | string | 必填/键名 | 模型名；若填必须与 map 键一致。 |
| `MaxBatchSize` | int | `32` | 发往该模型的子批最大条目数；达到即**深度触发**。 |
| `BatchWindow` | duration | `5ms` | 队首请求最长等待，达到即**窗口触发**。 |
| `QueueCapacity` | int | `128` | 有界队列容量；系统内总占用（队列+在途）恒不超过它。 |
| `RequestTimeout` | duration | `30s` | 从入队到被批次消费的最大等待时间；超时结算为 `queue_timeout`。0 用默认值，不表示无限。 |
| `MaxRetries` | int | `0` | 可重试失败的**额外**尝试次数（总尝试 `MaxRetries+1`）。 |
| `RetryStrategy` | enum | `never` | `never` / `immediate` / `requeue`，语义见 batching.md。 |
| `OverflowPolicy` | enum | `reject` | `reject`（立即 `queue_full`）/ `block`（等待空位）。 |

> 注：上表 `QueueCapacity` 默认值以代码常量为准（`128`）。

零值字段会在 `Config.normalize()` 填充默认值，但热更新时建议显式写全。
若设置了 `MaxRetries>0` 却用 `RetryStrategy=never`，校验直接拒绝。

## 3. Config

```go
type Config struct {
    Version int64                          // 必须为正；热更新需严格递增
    Global  GlobalConfig
    Models  map[string]ModelConfig        // 完整模型集合（全量替换语义）
}
```

配置是**全量**模型集合：未出现在新配置中的模型视为移除。
校验/版本/收缩规则见 [scheduling.md](scheduling.md) 第 4 节。

## 4. 可观测：SchedulerStats / ModelStats

`Scheduler.Stats()` 返回值类型快照（拷贝，可安全读取）：

全局：

- `Version`：当前生效配置版本；
- `UptimeMillis`：运行时长；
- `InFlight`：已出发批次中尚未结算的条目总数；
- `BatchesSent`：混合批次总数；`BatchItems`：累计批内条目数；
- `BatchLatencyMicros`：批延迟样本（有界，最多 1024 个，已排序）；
- `BatchLatencyPercentiles`：`p50/p90/p99`（微秒，无样本时为 -1）。

每模型 `ModelStats`：

| 字段 | 含义 |
| --- | --- |
| `QueueDepth` / `InFlight` / `Outstanding` / `QueueCap` | 在队 / 在途 / 总占用 / 容量，`Outstanding<=QueueCap` |
| `Submitted` | 成功入队数 |
| `Completed` | 成功返还结果数 |
| `Rejected` | 入队阶段拒绝数（满、关闭） |
| `TimedOut` / `Canceled` | 超时 / 取消结算数 |
| `Failed` | 失败结算数（含重试耗尽） |
| `RetryExh` | 其中重试耗尽的数量 |
| `Retried` | 触发的重试动作次数 |
| `BatchesSent` | 该模型子批数 |
| `BatchSizeDist` | 子批大小 → 次数（批大小分布直方图） |

## 5. 错误原因速查

见 README 的“错误结论（Reason）”。所有对外错误都可 `errors.As` 为
`*APIError` 并按 `Reason` 分支处理，便于上游做重试/降级/告警。

## 6. 典型调参建议

- 追求低单请求延迟：小 `BatchWindow`（1~5ms）+ 适中 `MaxBatchSize`；
- 追求高吞吐：加大窗口与批大小，配合 `OverflowBlock` 与较大容量；
- 需要强隔离/快速失败：`OverflowReject`，由上游按 `queue_full` 降载；
- 外部推理偶发抖动：`RetryImmediate` + `MaxRetries=1~2`；
  需要把重试机会让给后续流量：`RetryRequeue`。
