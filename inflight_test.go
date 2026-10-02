package serving

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateEngine 在 enter 处阻塞直到 open，用于把批次稳定地停在执行中。
type gateEngine struct {
	stub  *StubEngine
	mu    sync.Mutex
	model string
	enter chan struct{}
	open  chan struct{}
	calls int
}

func (e *gateEngine) Infer(ctx context.Context, model string, payloads []any) ([]InferResult, error) {
	e.mu.Lock()
	if model == e.model {
		e.calls++
		select {
		case e.enter <- struct{}{}:
		default:
		}
	}
	e.mu.Unlock()
	if model == e.model {
		select {
		case <-e.open:
		case <-ctx.Done():
		}
	}
	return e.stub.Infer(ctx, model, payloads)
}

// TestRemoveModelWhileBatchInFlight 验证批次执行中热更新移除模型：
// 在途请求执行完成后仍准确把结果返还原调用方，不被丢弃也不重复计算。
func TestRemoveModelWhileBatchInFlight(t *testing.T) {
	stub := NewStubEngine()
	gate := &gateEngine{stub: stub, model: "m", enter: make(chan struct{}, 1), open: make(chan struct{})}

	mc := baseModel("m")
	mc.MaxBatchSize = 2
	mc.BatchWindow = 5 * time.Millisecond
	s, err := New(testConfig(1, mc), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c1, _ := s.SubmitChan(context.Background(), Request{Model: "m", Payload: "a"})
	c2, _ := s.SubmitChan(context.Background(), Request{Model: "m", Payload: "b"})

	select {
	case <-gate.enter:
	case <-time.After(time.Second):
		t.Fatal("batch did not enter engine")
	}

	// 执行中移除模型（在队为空，收缩检查可通过）。
	empty := testConfig(2) // 不含任何模型
	if err := s.UpdateConfig(empty); err != nil {
		t.Fatal(err)
	}
	// 新提交立即 model_not_found。
	if _, err := s.Submit(context.Background(), Request{Model: "m", Payload: "z"}); reasonOf(err) != ReasonModelNotFound {
		t.Fatalf("want model_not_found, got %v", err)
	}

	// 放开门，在途批次完成，两个原请求都应得到成功结果。
	close(gate.open)
	r1 := waitResult(t, c1)
	r2 := waitResult(t, c2)
	if r1.Err != nil || r2.Err != nil {
		t.Fatalf("inflight requests must complete with results, got %v %v", r1.Err, r2.Err)
	}
	if r1.Val.(EchoOutput).Value != "a" || r2.Val.(EchoOutput).Value != "b" {
		t.Fatalf("inflight result mismatch %v %v", r1.Val, r2.Val)
	}
}

// TestInFlightRetryAfterModelRemoved 验证 requeue 重试时模型已移除：
// 在途重试条目以 queue_closed 确定性结算，不丢失。
func TestInFlightRetryAfterModelRemoved(t *testing.T) {
	stub := NewStubEngine()
	stub.SetBehavior("m", &StubBehavior{FailWholeBatch: true, Retryable: true})
	gate := &gateEngine{stub: stub, model: "m", enter: make(chan struct{}, 4), open: make(chan struct{})}

	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.BatchWindow = 3 * time.Millisecond
	mc.MaxRetries = 5
	mc.RetryStrategy = RetryRequeue
	s, err := New(testConfig(1, mc), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ch, _ := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 1})
	select {
	case <-gate.enter:
	case <-time.After(time.Second):
		t.Fatal("batch did not enter")
	}
	// 首次 Infer 还在门内时移除模型，随后放行；失败项无法回队，
	// 应被结算为 queue_closed 或因重试路径关闭而结束。
	if err := s.UpdateConfig(testConfig(2)); err != nil {
		t.Fatal(err)
	}
	close(gate.open)
	select {
	case r := <-ch:
		if r.Err == nil {
			t.Fatal("removed-model inflight retry must conclude with an error")
		}
		switch reasonOf(r.Err) {
		case ReasonQueueClosed, ReasonRetryExhausted, ReasonFailed:
		default:
			t.Fatalf("unexpected reason %v", r.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inflight retry not concluded after model removal")
	}
}
