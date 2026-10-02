package serving

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testConfig(version int64, models ...ModelConfig) Config {
	m := make(map[string]ModelConfig, len(models))
	for _, mc := range models {
		m[mc.Name] = mc
	}
	return Config{
		Version: version,
		Global:  GlobalConfig{TickInterval: time.Millisecond, NumWorkers: 2},
		Models:  m,
	}
}

func baseModel(name string) ModelConfig {
	return ModelConfig{
		Name:           name,
		MaxBatchSize:   8,
		BatchWindow:    20 * time.Millisecond,
		QueueCapacity:  64,
		RequestTimeout: 2 * time.Second,
		RetryStrategy:  RetryNever,
		OverflowPolicy: OverflowReject,
	}
}

func waitResult(t *testing.T, ch <-chan Response) Response {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting result")
		return Response{}
	}
}

// TestDynamicBatching_WindowAndDepth 验证：
//   - 窗口未到但达到 MaxBatchSize 时立即触发（深度组批）；
//   - 窗口到期时即使不满也触发（窗口组批）；
//   - 一次 Infer 覆盖整批成员，结果按原请求准确返还。
func TestDynamicBatching_WindowAndDepth(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 4
	mc.BatchWindow = 80 * time.Millisecond
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	submit := func(v string) <-chan Response {
		ch, err := s.SubmitChan(context.Background(), Request{Model: "m", Payload: v})
		if err != nil {
			t.Fatal(err)
		}
		return ch
	}

	// 深度触发：连续 4 个（>=MaxBatchSize）应很快合成一批。
	chs := make([]<-chan Response, 4)
	for i := range chs {
		chs[i] = submit("d")
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if engine.CallCount("m") >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if calls := engine.CallCount("m"); calls != 1 {
		t.Fatalf("depth trigger: want 1 infer call, got %d", calls)
	}
	for i, ch := range chs {
		r := waitResult(t, ch)
		if r.Err != nil {
			t.Fatalf("req %d unexpected err: %v", i, r.Err)
		}
		out := r.Val.(EchoOutput)
		if out.Value != "d" {
			t.Fatalf("payload mismatch: %v", out)
		}
	}

	// 窗口触发：只发 2 个，等待 BatchWindow 后也应被处理。
	c1 := submit("w")
	c2 := submit("w")
	if r := waitResult(t, c1); r.Err != nil {
		t.Fatalf("window req1: %v", r.Err)
	}
	if r := waitResult(t, c2); r.Err != nil {
		t.Fatalf("window req2: %v", r.Err)
	}
	if calls := engine.CallCount("m"); calls < 2 {
		t.Fatalf("window trigger: want >=2 calls, got %d", calls)
	}

	st := s.Stats()
	var completed int64
	var dist map[int]int64
	for _, ms := range st.Models {
		if ms.Model == "m" {
			completed = ms.Completed
			dist = ms.BatchSizeDist
		}
	}
	if completed != 6 {
		t.Fatalf("want 6 completed, got %d", completed)
	}
	if dist[4] < 1 || dist[2] < 1 {
		t.Fatalf("batch size distribution want >=1 of size 4 and size 2, got %v", dist)
	}
}

// TestDynamicBatching_MixedModels 验证一个批次混合多个模型：
// 同一拍内两个模型各触发窗口，产生一个混合批次，含两个子批。
func TestDynamicBatching_MixedModels(t *testing.T) {
	engine := NewStubEngine()
	a := baseModel("aaa")
	b := baseModel("bbb")
	s, err := New(testConfig(1, a, b), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ca, err := s.SubmitChan(context.Background(), Request{Model: "aaa", Payload: 1})
	if err != nil {
		t.Fatal(err)
	}
	cb, err := s.SubmitChan(context.Background(), Request{Model: "bbb", Payload: 2})
	if err != nil {
		t.Fatal(err)
	}
	ra := waitResult(t, ca)
	rb := waitResult(t, cb)
	if ra.Err != nil || rb.Err != nil {
		t.Fatalf("unexpected errors: %v %v", ra.Err, rb.Err)
	}
	if ra.Val.(EchoOutput).Model != "aaa" || rb.Val.(EchoOutput).Model != "bbb" {
		t.Fatalf("result-model mismatch: %v %v", ra.Val, rb.Val)
	}
	if engine.CallCount("aaa") != 1 || engine.CallCount("bbb") != 1 {
		t.Fatalf("want exactly 1 call per model, got aaa=%d bbb=%d",
			engine.CallCount("aaa"), engine.CallCount("bbb"))
	}
	if st := s.Stats(); st.BatchesSent != 1 {
		t.Fatalf("want 1 mixed batch, got %d", st.BatchesSent)
	}
}

// TestErrorsReason 验证错误原因可通过 errors.As 区分。
func TestErrorsReason(t *testing.T) {
	engine := NewStubEngine()
	s, err := New(testConfig(1, baseModel("m")), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Submit(context.Background(), Request{Model: "nope", Payload: 1})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Reason != ReasonModelNotFound {
		t.Fatalf("want model_not_found, got %v", err)
	}
}

// TestConcurrentSubmitExactlyOnce 高并发下验证每个请求恰好结算一次、
// 结果与原请求一一对应，无重复计算/错配。
func TestConcurrentSubmitExactlyOnce(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 16
	mc.OverflowPolicy = OverflowBlock
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 500
	var wg sync.WaitGroup
	results := make([]int, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			out, err := s.Submit(context.Background(), Request{Model: "m", Payload: i})
			errs[i] = err
			if err == nil {
				results[i] = out.(EchoOutput).Value.(int)
			}
		}(i)
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent submit timed out")
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("req %d err: %v", i, errs[i])
		}
		if results[i] != i {
			t.Fatalf("result mismatch at %d: got %d", i, results[i])
		}
	}
	st := s.Stats()
	for _, ms := range st.Models {
		if ms.Model == "m" && ms.Completed != n {
			t.Fatalf("want %d completed, got %d", n, ms.Completed)
		}
	}
}
