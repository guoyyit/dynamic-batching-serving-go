# dynamic-batching-serving-go

纯进程内的多模型**动态批处理（dynamic batching）推理调度**参考实现。
整条链路（提交 → 排队 → 组批 → 执行 → 结果返还）只在本进程内运行，
推理由确定性的本地桩 `StubEngine` 完成，不依赖任何外部服务；
所有成功结果、错误结论与延迟都可在本地稳定复现。

## 能力一览

| 需求 | 实现位置 |
| --- | --- |
| 多模型共用一个调度入口，队列/参数隔离，拥塞互不挤占 | `scheduler.go`、`registry.go`、`queue.go` |
| 按批窗口 + 队列深度动态组批，单批可混合多模型 | `scheduler.go::formAndDispatch`、`batch.go` |
| 排队等待、超时与取消得到**可区分**结论 | `errors.go`（Reason）、`queue.go`、`scheduler.go` |
| 并发提交 + 固定多工作协程消费，每请求恰好消费/结算一次 | `worker.go`、`request.go::settle(sync.Once)` |
| 执行失败 / 部分失败隔离，重试不重复副作用 | `worker.go::executeGroup`、`stub.go` |
| 背压：有界队列 + reject/block 策略，拒绝原因可区分 | `queue.go`、`config.go` |
| 运行时观测与配置原子热更新 | `metrics.go`、`registry.go::apply` |
| 自动化测试覆盖全部场景 | `*_test.go` |

## 快速开始

```go
engine := serving.NewStubEngine() // 本地确定性桩，也可实现 serving.Inferencer
s, _ := serving.New(serving.Config{
    Version: 1,
    Global:  serving.GlobalConfig{TickInterval: 2 * time.Millisecond, NumWorkers: 4},
    Models: map[string]serving.ModelConfig{
        "ranker": {
            Name: "ranker", MaxBatchSize: 32, BatchWindow: 5 * time.Millisecond,
            QueueCapacity: 256, RequestTimeout: time.Second,
            RetryStrategy: serving.RetryImmediate, MaxRetries: 1,
            OverflowPolicy: serving.OverflowBlock,
        },
    },
}, engine)
defer s.Close()

out, err := s.Submit(ctx, serving.Request{Model: "ranker", Payload: myInput})
```

运行示例：`go run ./cmd/demo`
运行测试：`go test -race ./...`

## 文档

- [docs/batching.md](docs/batching.md)：批处理语义——合并/拆分规则、顺序、版本一致性。
- [docs/scheduling.md](docs/scheduling.md)：调度规则——触发条件、隔离、背压、重试、关闭。
- [docs/configuration.md](docs/configuration.md)：配置项、默认值、热更新语义、观测指标。

## 错误结论（Reason）

`Submit` 返回的错误可通过 `errors.As` 取 `*serving.APIError`，其 `Reason`：

- `model_not_found` / `scheduler_closed` / `invalid_config`
- `queue_full`（reject 背压）/ `queue_closed`（模型移除或关闭）
- `queue_timeout`（等待超时）/ `canceled`（调用方取消）
- `failed`（执行失败且未重试/不可重试）/ `retry_exhausted`（重试耗尽）
