package serving

import (
	"context"
	"testing"
	"time"
)

// slowInferencer 对 slow 模型注入高延迟，fast 模型立即返回，
// 验证慢模型拥塞不会让快模型饿死或全局调度停摆。
type slowInferencer struct {
	stub *StubEngine
}

func (e *slowInferencer) Infer(ctx context.Context, model string, payloads []any) ([]InferResult, error) {
	if model == "slow" {
		select {
		case <-time.After(40 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	return e.stub.Infer(ctx, model, payloads)
}

// TestMultiModelIsolation_QueueAndCongestion 验证：
//  1. 一个模型队列满（reject）只拒绝它自己，不影响另一模型提交；
//  2. 一个模型执行很慢（占满工作协程为边界情形）时，另一模型仍能被调度；
//  3. 队列深度按模型独立统计。
func TestMultiModelIsolation_QueueAndCongestion(t *testing.T) {
	engine := &slowInferencer{stub: NewStubEngine()}

	// slow 容量小且 reject；fast 容量充足。
	slow := baseModel("slow")
	slow.QueueCapacity = 2
	slow.MaxBatchSize = 1
	slow.BatchWindow = 5 * time.Millisecond
	fast := baseModel("fast")
	fast.MaxBatchSize = 1
	fast.BatchWindow = 5 * time.Millisecond
	s, err := New(testConfig(1, slow, fast), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 用 ctx 保证这些 slow 请求最终会超时，避免堆积。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	accepted, rejected := 0, 0
	for i := 0; i < 20; i++ {
		ch, err := s.SubmitChan(ctx, Request{Model: "slow", Payload: i})
		if err != nil {
			if ae, ok := err.(*APIError); !ok || ae.Reason != ReasonQueueFull {
				t.Fatalf("want queue_full, got %v", err)
			}
			rejected++
			continue
		}
		accepted++
		go func(c <-chan Response) { <-c }(ch) // 消费结果，避免通道引用悬挂
	}
	if rejected == 0 {
		t.Fatalf("expected slow model queue rejections, accepted=%d rejected=%d", accepted, rejected)
	}

	// fast 模型在 slow 拥塞期间必须仍然可用。
	fch, err := s.SubmitChan(context.Background(), Request{Model: "fast", Payload: "ok"})
	if err != nil {
		t.Fatalf("fast model must remain available, got: %v", err)
	}
	r := waitResult(t, fch)
	if r.Err != nil {
		t.Fatalf("fast request failed: %v", r.Err)
	}
	if r.Val.(EchoOutput).Value != "ok" {
		t.Fatalf("fast result mismatch: %v", r.Val)
	}

	st := s.Stats()
	depths := map[string]int{}
	for _, ms := range st.Models {
		depths[ms.Model] = ms.QueueDepth
	}
	if _, ok := depths["slow"]; !ok {
		t.Fatal("missing slow queue depth")
	}
	if _, ok := depths["fast"]; !ok {
		t.Fatal("missing fast queue depth")
	}
}

// TestMultiModelIsolation_IndependentParams 验证发往不同模型的批次
// 使用各自的 MaxBatchSize：大窗口模型不因小窗口模型而提前或滞后。
func TestMultiModelIsolation_IndependentParams(t *testing.T) {
	engine := NewStubEngine()
	big := baseModel("big")
	big.MaxBatchSize = 4
	big.BatchWindow = 100 * time.Millisecond
	small := baseModel("small")
	small.MaxBatchSize = 1
	small.BatchWindow = 5 * time.Millisecond

	s, err := New(testConfig(1, big, small), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	cb, err := s.SubmitChan(context.Background(), Request{Model: "big", Payload: "b"})
	if err != nil {
		t.Fatal(err)
	}
	cs, err := s.SubmitChan(context.Background(), Request{Model: "small", Payload: "s"})
	if err != nil {
		t.Fatal(err)
	}
	// small 应很快返回，big 因窗口 100ms 且只有 1 个请求仍在等待。
	if r := waitResult(t, cs); r.Err != nil {
		t.Fatalf("small: %v", r.Err)
	}
	if engine.CallCount("big") != 0 {
		t.Fatalf("big model should not be sent before its window, calls=%d", engine.CallCount("big"))
	}
	if r := waitResult(t, cb); r.Err != nil {
		t.Fatalf("big: %v", r.Err)
	}
}
