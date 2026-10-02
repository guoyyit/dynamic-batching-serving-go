# 调度规则、隔离、背压与关闭

## 1. 运行时结构

```
Submit ─► [模型 A 有界 FIFO] ─┐
Submit ─► [模型 B 有界 FIFO] ─┼─► 1 个调度循环（动态组批）
Submit ─► [模型 C 有界 FIFO] ─┘            │
                                          ▼
                          容量=NumWorkers 的批次通道
                                          │
                 ┌────────────┬───────────┴───────────┐
              worker-0     worker-1 ... worker-N     （固定数量）
                                          │
                                  StubEngine (Inferencer)
```

常驻 goroutine 数量 = `1（调度循环） + NumWorkers`，不随负载增加。
`Submit` 在 block 策略下的等待只发生在调用方自己的 goroutine 上，
系统不额外创建“每请求定时器/协程”。

## 2. 多模型隔离

- 每个模型有独立的有界队列（`QueueCapacity`）与独立配置；
- 一个模型队列满只拒绝/阻塞该模型的提交，其他模型照常（测试
  `TestMultiModelIsolation_QueueAndCongestion`）；
- 一个模型执行很慢时：调度循环**不执行引擎**，只负责组批投递，
  因此不会被慢模型阻塞；当批次通道（容量=worker 数）被慢批占满时，
  新批次会把各子批**原样放回队首**并稍后重试，调度循环继续运转，
  慢模型不会让全局调度停摆；
- 慢模型的大窗口不会拖累小窗口模型（`TestMultiModelIsolation_IndependentParams`）。

## 3. 背压与资源有界性

容量以“系统内总占用”计，而不仅是队列长度：

```
每个模型任意时刻：len(queue) + outstanding == 占用名额 <= QueueCapacity
```

- `outstanding` = 已出队但尚未得到最终结论（或尚未回队）的条目；
- 条目出队即占用在途名额，成功/失败/超时/取消结算后 `release`；
- 回队重试把在途名额**转回**队列槽位，总量不变；
- 出队数量还受“剩余在途名额”限制，保证任何交错下都不超限
  （代码中的 `checkInvariantLocked` 断言这一点）。

`OverflowPolicy`：

- `reject`：满时立即返回 `queue_full`（默认，最常用的背压信号）；
- `block`：等待空位。等待通过 `sync.Cond` 实现，唤醒来源有三种：
  1. 有条目出队/回队/结算释放名额；
  2. 队列关闭或模型移除；
  3. 调度循环每个节拍 `notifyWaiters()`，使取消/超时最迟在一个节拍内
     被感知——无需为每个请求创建定时器或监控协程。

拒绝、超时、取消分别以 `queue_full / queue_timeout / canceled` 返回，
调用方可区分处理（`TestRejectBackpressure`、`TestBlockQueueCanceled`）。

## 4. 配置热更新（版本原子性）

`UpdateConfig(Config)` 走 `registry.apply`，与组批共享一把 RWMutex：

- 调度循环在 **RLock 下**取得某版本下全部模型 entry 的一致快照后组批；
- 更新在 **Lock 下**一次性完成队列的新建/迁移/关闭；
- 因此进行中的组批看到的要么全是旧版本、要么全是新版本，
  绝不会出现“半新半旧”的混合参数批次；
- 批次自身还固化组批时刻的参数与队列引用，执行期间的更新不影响在途批。

更新被接受的条件（任一不满足则返回 `invalid_config`，旧版本原样保留）：

1. `Config.Validate()` 通过（正数版本、枚举合法、无负值等）；
2. `Version` 严格大于当前版本（防止旧快照覆盖新配置）；
3. 对每个保留模型，`新容量 >= 该模型当前在队长度`（拒绝过早收缩）。

变更效果：

- **新增模型**：创建空队列，立即可提交；
- **移除模型**：关闭其队列，在队请求结算为 `queue_closed`，之后提交
  返回 `model_not_found`；
- **容量不变**：复用同一队列，FIFO 连续；
- **容量变化**：新建队列，原子迁移全部在队条目与在途名额，保持 FIFO。

`Stats().Version` / `CurrentVersion()` 可确认当前生效版本。

## 5. 关闭语义

`Close()` 可重复调用，顺序为：

1. 标记关闭，停止调度循环，此后 `Submit` 返回 `scheduler_closed`；
2. 关闭批次通道并等待全部在途批次执行完、结算完（不中断引擎调用）；
3. 关闭所有模型队列：仍在排队的请求全部结算为 `queue_closed`。

不存在“已经组批却被静默丢弃”的请求（`TestBatchNotSilentlyDropped`、
`TestCloseRejectsAndSettles`）。

## 6. 本地桩 StubEngine

`NewStubEngine()` 返回确定性后端：成功时输出 `EchoOutput{Model,Value,
Call,Index}`，下标/调用次数可断言；`SetBehavior` 可注入：

- `FailWholeBatch` + `Retryable`：整批可重试/不可重试失败；
- `FailIndices`：按下标的部分失败；
- `FailFirstCalls`：故障仅前 N 次调用生效，之后恢复——用于确定性
  制造“重试后成功”；
- `Latency`：确定性延迟（先检查 ctx）。

全部测试只用内存 + 这个桩，结果与错误在本地可复现。
