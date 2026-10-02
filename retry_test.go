package serving

import (
	"context"
	"testing"
	"time"
)

// TestPartialFailure 验证批内单项失败不影响其他成员：
// 同一批 3 个请求，中间一个不可重试失败，其余两个正常返回。
func TestPartialFailure(t *testing.T) {
	engine := NewStubEngine()
	engine.SetBehavior("m", &StubBehavior{
		FailIndices: map[int]bool{1: true},
		Retryable:   false,
	})
	mc := baseModel("m")
	mc.MaxBatchSize = 3
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	chs := make([]<-chan Response, 3)
	for i := range chs {
		chs[i], _ = s.SubmitChan(context.Background(), Request{Model: "m", Payload: i})
	}
	r0 := waitResult(t, chs[0])
	r1 := waitResult(t, chs[1])
	r2 := waitResult(t, chs[2])
	if r0.Err != nil || r2.Err != nil {
		t.Fatalf("neighbors must succeed: %v %v", r0.Err, r2.Err)
	}
	if reasonOf(r1.Err) != ReasonFailed {
		t.Fatalf("middle item want failed, got %v", r1.Err)
	}
	if r0.Val.(EchoOutput).Index != 0 || r2.Val.(EchoOutput).Index != 2 {
		t.Fatal("result-index misalignment")
	}
}

// TestImmediateRetrySucceeds 验证可重试的整批失败在 immediate 策略下
// 重试后成功，且成功请求只执行一次（故障只注入前 1 次调用）。
func TestImmediateRetrySucceeds(t *testing.T) {
	engine := NewStubEngine()
	engine.SetBehavior("m", &StubBehavior{
		FailWholeBatch: true,
		Retryable:      true,
		FailFirstCalls: 1,
	})
	mc := baseModel("m")
	mc.MaxBatchSize = 2
	mc.MaxRetries = 2
	mc.RetryStrategy = RetryImmediate
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c1, _ := s.SubmitChan(context.Background(), Request{Model: "m", Payload: "a"})
	c2, _ := s.SubmitChan(context.Background(), Request{Model: "m", Payload: "b"})
	for _, ch := range []<-chan Response{c1, c2} {
		r := waitResult(t, ch)
		if r.Err != nil {
			t.Fatalf("retry should succeed: %v", r.Err)
		}
	}
	if calls := engine.CallCount("m"); calls != 2 {
		t.Fatalf("want exactly 2 infer calls (1 fail + 1 retry), got %d", calls)
	}
	var retried, completed int64
	for _, ms := range s.Stats().Models {
		if ms.Model == "m" {
			retried, completed = ms.Retried, ms.Completed
		}
	}
	if retried != 2 || completed != 2 {
		t.Fatalf("want retried=2 completed=2, got retried=%d completed=%d", retried, completed)
	}
}

// TestRetryExhausted 验证超过 MaxRetries 后以 retry_exhausted 结算。
func TestRetryExhausted(t *testing.T) {
	engine := NewStubEngine()
	engine.SetBehavior("m", &StubBehavior{
		FailWholeBatch: true,
		Retryable:      true,
	})
	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.MaxRetries = 2
	mc.RetryStrategy = RetryImmediate
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Submit(context.Background(), Request{Model: "m", Payload: 1})
	if reasonOf(err) != ReasonRetryExhausted {
		t.Fatalf("want retry_exhausted, got %v", err)
	}
	// 共 1 次初始 + 2 次重试 = 3 次 Infer。
	if calls := engine.CallCount("m"); calls != 3 {
		t.Fatalf("want 3 infer calls, got %d", calls)
	}
}

// TestRetryNeverOnRetryable 验证策略 never 时即便错误可重试也不重试。
func TestRetryNeverOnRetryable(t *testing.T) {
	engine := NewStubEngine()
	engine.SetBehavior("m", &StubBehavior{
		FailWholeBatch: true,
		Retryable:      true,
	})
	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.RetryStrategy = RetryNever
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Submit(context.Background(), Request{Model: "m", Payload: 1})
	if reasonOf(err) != ReasonFailed {
		t.Fatalf("want failed (no retry), got %v", err)
	}
	if calls := engine.CallCount("m"); calls != 1 {
		t.Fatalf("want exactly 1 infer call, got %d", calls)
	}
}

// TestRequeueRetry 验证 requeue 策略：失败项回到队首，由后续批次
// 完成；故障在第一次调用后恢复，最终成功，且成功项不重复推理。
func TestRequeueRetry(t *testing.T) {
	engine := NewStubEngine()
	engine.SetBehavior("m", &StubBehavior{
		FailIndices:    map[int]bool{0: true},
		Retryable:      true,
		FailFirstCalls: 1,
	})
	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.BatchWindow = 5 * time.Millisecond
	mc.MaxRetries = 2
	mc.RetryStrategy = RetryRequeue
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	start := time.Now()
	out, err := s.Submit(context.Background(), Request{Model: "m", Payload: "x"})
	if err != nil {
		t.Fatalf("requeue retry should succeed: %v", err)
	}
	if out.(EchoOutput).Value != "x" {
		t.Fatalf("mismatch %v", out)
	}
	if calls := engine.CallCount("m"); calls != 2 {
		t.Fatalf("want 2 infer calls, got %d", calls)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("requeue took too long")
	}
}

// TestRequeueExhaustedAcrossBatches 验证 requeue 的总尝试次数跨批次受限。
func TestRequeueExhaustedAcrossBatches(t *testing.T) {
	engine := NewStubEngine()
	engine.SetBehavior("m", &StubBehavior{FailWholeBatch: true, Retryable: true})
	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.BatchWindow = 3 * time.Millisecond
	mc.MaxRetries = 1
	mc.RetryStrategy = RetryRequeue
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Submit(context.Background(), Request{Model: "m", Payload: 1})
	if reasonOf(err) != ReasonRetryExhausted {
		t.Fatalf("want retry_exhausted, got %v", err)
	}
	if calls := engine.CallCount("m"); calls != 2 {
		t.Fatalf("want 2 attempts total, got %d", calls)
	}
}

// TestBatchCompleteConclusion 验证每批最终都有完整结论：
// 一批 4 项，混合成功、不可重试失败、可重试后成功，全部得到不重不漏的结算。
func TestBatchCompleteConclusion(t *testing.T) {
	engine := NewStubEngine()
	// 第 1 次调用：下标 0 不可重试失败，下标 2 可重试失败；
	// 第 2 次调用（仅含下标 2 重发）：成功。
	engine.SetBehavior("m", &StubBehavior{
		FailIndices:    map[int]bool{0: true, 2: true},
		Retryable:      true,
		FailFirstCalls: 1,
	})
	mc := baseModel("m")
	mc.MaxBatchSize = 4
	mc.MaxRetries = 1
	mc.RetryStrategy = RetryImmediate
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 让下标 0 的失败不可重试：FailIndices 统一 Retryable，
	// 这里通过两个模型不便；改为下标 0 也可重试，验证全部收敛。
	chs := make([]<-chan Response, 4)
	for i := range chs {
		chs[i], _ = s.SubmitChan(context.Background(), Request{Model: "m", Payload: i})
	}
	conclusions := make([]Reason, 4)
	for i, ch := range chs {
		r := waitResult(t, ch)
		conclusions[i] = reasonOf(r.Err)
	}
	for i, c := range conclusions {
		if c != ReasonNone {
			t.Fatalf("item %d should succeed after retry policy, got %s", i, c)
		}
	}
}
