package serving

import (
	"context"
	"sync"
	"time"
)

// Inferencer 是本地推理后端的最小接口。整个链路只在本进程内运行，
// 由本地桩 StubEngine 实现；测试可用自定义实现注入故障与延迟。
//
// 契约（调度器依赖的确定性规则）：
//   - 返回的 results 与 payloads 等长且下标一一对应；单项失败通过
//     results[i].Err 表达（部分失败），其余下标结果仍然有效；
//   - 整批无法给出逐项结果时返回非 nil error；若该 error 为
//     *InferError 且 Retryable=true，调度器按模型策略重试整批中尚未
//     成功的成员；成功成员永远不会被重复送入 Infer。
type Inferencer interface {
	Infer(ctx context.Context, model string, payloads []any) ([]InferResult, error)
}

// InferResult 是单项推理的结果，下标由执行方保证与输入顺序对齐。
type InferResult struct {
	Model  string
	Output any
	// Err 非 nil 表示该单项失败（部分失败）。
	Err *InferError
}

// InferError 区分可重试/不可重试的执行错误。
type InferError struct {
	Retryable bool
	Msg       string
}

func (e *InferError) Error() string {
	if e.Retryable {
		return "retryable inference error: " + e.Msg
	}
	return "inference error: " + e.Msg
}

// EchoOutput 是桩成功时返回的确定性输出，便于测试断言结果未错配。
type EchoOutput struct {
	Model string
	Value any
	Call  int // 该模型第几次 Infer 调用
	Index int // 本次调用中的下标
}

// StubBehavior 描述桩对某模型故障注入的确定性规则。
type StubBehavior struct {
	// FailWholeBatch=true 时每次（或前 FailFirstCalls 次）调用整批失败。
	FailWholeBatch bool
	// FailIndices 列出调用内需要失败的下标（部分失败）。
	FailIndices map[int]bool
	// Retryable 决定注入的失败是否可重试。
	Retryable bool
	// FailFirstCalls>0 时，故障只对前 N 次调用生效，之后恢复正常，
	// 用于确定性地制造“重试后成功”。0 表示始终生效。
	FailFirstCalls int
	// Latency 为每次调用增加的确定性延迟（先检查 ctx）。
	Latency time.Duration
}

// StubEngine 是确定性的本地推理桩：
// 正常情况下把每个 payload 包装为 EchoOutput；可按 model 注册故障行为。
type StubEngine struct {
	mu        sync.Mutex
	behaviors map[string]*StubBehavior
	calls     map[string]int
}

// NewStubEngine 创建桩引擎。
func NewStubEngine() *StubEngine {
	return &StubEngine{
		behaviors: make(map[string]*StubBehavior),
		calls:     make(map[string]int),
	}
}

// SetBehavior 设置某模型的故障行为；传 nil 清除。返回该模型累计调用次数。
func (s *StubEngine) SetBehavior(model string, b *StubBehavior) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b == nil {
		delete(s.behaviors, model)
	} else {
		cp := *b
		s.behaviors[model] = &cp
	}
	return s.calls[model]
}

// CallCount 返回某模型的累计 Infer 调用次数。
func (s *StubEngine) CallCount(model string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[model]
}

// Infer 实现 Inferencer。
func (s *StubEngine) Infer(ctx context.Context, model string, payloads []any) ([]InferResult, error) {
	s.mu.Lock()
	var b *StubBehavior
	if sb, ok := s.behaviors[model]; ok {
		cp := *sb
		b = &cp
	}
	call := s.calls[model] + 1
	s.calls[model] = call
	s.mu.Unlock()

	if b != nil && b.Latency > 0 {
		select {
		case <-time.After(b.Latency):
		case <-ctx.Done():
			return nil, &InferError{Retryable: true, Msg: "stub interrupted: " + ctx.Err().Error()}
		}
	}

	active := b != nil && (b.FailFirstCalls == 0 || call <= b.FailFirstCalls)
	if active && b.FailWholeBatch {
		return nil, &InferError{Retryable: b.Retryable, Msg: "injected whole-batch failure"}
	}

	results := make([]InferResult, len(payloads))
	for i, p := range payloads {
		if active && b.FailIndices != nil && b.FailIndices[i] {
			results[i] = InferResult{
				Model: model,
				Err:   &InferError{Retryable: b.Retryable, Msg: "injected item failure"},
			}
			continue
		}
		results[i] = InferResult{
			Model:  model,
			Output: EchoOutput{Model: model, Value: p, Call: call, Index: i},
		}
	}
	return results, nil
}

var _ Inferencer = (*StubEngine)(nil)
