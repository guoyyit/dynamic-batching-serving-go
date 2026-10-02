package sched

import (
	"errors"
	"sync"
	"time"
)

// 桩引擎使用的错误。
var (
	// ErrStubRetryable 模拟可重试的推理失败。
	ErrStubRetryable = errors.New("stub: retryable inference failure")
	// ErrStubPermanent 模拟不可重试的永久失败。
	ErrStubPermanent = errors.New("stub: permanent inference failure")
)

// ItemResult 是单条请求的推理结论。
type ItemResult struct {
	ReqID     string
	Output    []byte
	Err       error
	Retryable bool
}

// Engine 是推理执行接口。给定同一模型的一组请求，逐条返回结论。
// 实现必须保证：对同一请求的重复调用不会产生重复副作用（幂等）。
type Engine interface {
	Infer(model string, reqs []*Request) []*ItemResult
}

// StubEngine 是本地桩实现，不依赖任何外部服务：
//   - 推理结果确定（模型名 + 输入回显）；
//   - 可按请求注入“先失败 N 次再成功”的可重试失败与永久失败；
//   - 记录每个请求的实际执行次数与副作用次数，用于断言重试不产生重复副作用。
type StubEngine struct {
	mu sync.Mutex

	// attempts 每个请求的实际调用次数。
	attempts map[string]int
	// sideEffects 每个请求的副作用次数（幂等消费者：多次重试只计一次）。
	sideEffects map[string]int
	// failUntil 每个请求前 N 次调用返回可重试失败。
	failUntil map[string]int
	// permanent 标记永久失败的请求。
	permanent map[string]bool
	// batches 记录每次 Infer 收到的请求 ID 列表，用于断言组批。
	batches [][]string
	// delay 每次调用的处理时长（模拟耗时推理，用于队列堆积/超时测试）。
	delay time.Duration
	// gate 非空时，每次推理调用阻塞在该通道上，直到被关闭释放（确定性测试）。
	gate chan struct{}
}

// SetDelay 设置每次推理调用的耗时。
func (e *StubEngine) SetDelay(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delay = d
}

// SetGate 启用门控：推理调用阻塞，直到 Release 被调用。
func (e *StubEngine) SetGate() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gate = make(chan struct{})
}

// Release 释放门控，所有阻塞的推理调用继续执行。
func (e *StubEngine) Release() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gate != nil {
		close(e.gate)
	}
}

// NewStubEngine 创建本地桩引擎。
func NewStubEngine() *StubEngine {
	return &StubEngine{
		attempts:    map[string]int{},
		sideEffects: map[string]int{},
		failUntil:   map[string]int{},
		permanent:   map[string]bool{},
	}
}

// SetFailUntil 配置请求 reqID 前 n 次调用返回可重试失败，第 n+1 次成功。
func (e *StubEngine) SetFailUntil(reqID string, n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failUntil[reqID] = n
}

// SetPermanentFail 配置请求永久失败（不可重试）。
func (e *StubEngine) SetPermanentFail(reqID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.permanent[reqID] = true
}

// Infer 逐条执行推理（桩）：结果确定，失败按注入规则返回。
func (e *StubEngine) Infer(model string, reqs []*Request) []*ItemResult {
	e.mu.Lock()
	delay := e.delay
	gate := e.gate
	e.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	ids := make([]string, 0, len(reqs))
	for _, r := range reqs {
		ids = append(ids, r.ID)
	}
	e.batches = append(e.batches, ids)

	results := make([]*ItemResult, 0, len(reqs))
	for _, r := range reqs {
		e.attempts[r.ID]++
		res := &ItemResult{ReqID: r.ID}
		switch {
		case e.permanent[r.ID]:
			res.Err = ErrStubPermanent
		case e.attempts[r.ID] <= e.failUntil[r.ID]:
			res.Err = ErrStubRetryable
			res.Retryable = true
		default:
			// 幂等副作用：同一请求多次重试只计一次。
			e.sideEffects[r.ID] = 1
			res.Output = []byte(model + ":" + string(r.Payload))
		}
		results = append(results, res)
	}
	return results
}

// Attempts 返回请求被实际执行的次数。
func (e *StubEngine) Attempts(reqID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts[reqID]
}

// SideEffects 返回请求产生的副作用次数（幂等消费者应为 1）。
func (e *StubEngine) SideEffects(reqID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sideEffects[reqID]
}

// Batches 返回历史上每次调用收到的请求 ID 列表。
func (e *StubEngine) Batches() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]string, len(e.batches))
	for i, b := range e.batches {
		out[i] = append([]string(nil), b...)
	}
	return out
}
