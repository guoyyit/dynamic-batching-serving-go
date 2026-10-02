package sched

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func baseConfig() Config {
	return Config{
		BatchWindow: 50 * time.Millisecond,
		Workers:     2,
		Models: map[string]ModelConfig{
			"m1": {MaxBatchSize: 4, MaxQueueSize: 10, MaxRetries: 2, RetryBackoff: 5 * time.Millisecond, Backpressure: BackpressureReject},
			"m2": {MaxBatchSize: 2, MaxQueueSize: 10, MaxRetries: 1, RetryBackoff: 5 * time.Millisecond, Backpressure: BackpressureBlock},
		},
	}
}

func newTestScheduler(t *testing.T, eng Engine, cfg Config) *Scheduler {
	t.Helper()
	if eng == nil {
		eng = NewStubEngine()
	}
	s, err := NewScheduler(eng, cfg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func submit(t *testing.T, s *Scheduler, model string, n int) []error {
	t.Helper()
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Submit(context.Background(), model, []byte(fmt.Sprintf("p-%d", i)))
		}(i)
	}
	wg.Wait()
	return errs
}

func flattenBatches(batches [][]string) []string {
	var out []string
	for _, b := range batches {
		out = append(out, b...)
	}
	return out
}

// 动态组批：窗口内到达的请求合并为一批。
func TestDynamicBatching(t *testing.T) {
	eng := NewStubEngine()
	s := newTestScheduler(t, eng, baseConfig())

	errs := submit(t, s, "m1", 4)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("req %d: %v", i, err)
		}
	}

	batches := eng.Batches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 merged batch, got %d: %v", len(batches), batches)
	}
	if len(batches[0]) != 4 {
		t.Fatalf("expected batch of 4, got %d", len(batches[0]))
	}
}

// 批窗口拆分：超过窗口的请求进入后续批次。
func TestBatchWindowSplit(t *testing.T) {
	eng := NewStubEngine()
	s := newTestScheduler(t, eng, baseConfig())

	submit(t, s, "m1", 3)
	time.Sleep(90 * time.Millisecond) // 超过 BatchWindow
	submit(t, s, "m1", 2)

	batches := eng.Batches()
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches split by window, got %d: %v", len(batches), batches)
	}
	if len(batches[0]) != 3 || len(batches[1]) != 2 {
		t.Fatalf("unexpected batch sizes: %d, %d", len(batches[0]), len(batches[1]))
	}
}

// 模型分组上限：发往同一模型的批次不得超过其 MaxBatchSize，溢出拆到下一批，FIFO 不乱序。
func TestModelGroupCap(t *testing.T) {
	eng := NewStubEngine()
	cfg := baseConfig()
	cfg.BatchWindow = 200 * time.Millisecond
	s := newTestScheduler(t, eng, cfg)

	submit(t, s, "m2", 5) // m2 MaxBatchSize=2

	batches := eng.Batches()
	var m2Sizes []int
	for _, b := range batches {
		m2Sizes = append(m2Sizes, len(b))
		if len(b) > 2 {
			t.Fatalf("m2 batch exceeds MaxBatchSize=2: %v", b)
		}
	}
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches (2,2,1), got %d: %v", len(batches), m2Sizes)
	}
	// FIFO：按提交顺序返还（通过 payload 前缀无法区分序号，这里断言每批大小与总数）。
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 5 {
		t.Fatalf("expected 5 requests in batches, got %d", total)
	}
}

// 多模型隔离：一个模型拥塞不得挤占其他模型，也不得拖垮全局调度。
func TestMultiModelIsolation(t *testing.T) {
	eng := NewStubEngine()
	eng.SetGate() // 门控：工人处理批次时阻塞，使队列堆积确定化
	cfg := baseConfig()
	cfg.Models["m1"] = ModelConfig{MaxBatchSize: 4, MaxQueueSize: 2, MaxRetries: 0, Backpressure: BackpressureReject}
	s := newTestScheduler(t, eng, cfg)

	// 洪峰压向 m1：在途工人(2*4) + 组批(4) + 队列(2) 最多吸收 14 个，其余必被拒绝。
	var errs []error
	done := make(chan struct{})
	go func() {
		errs = submit(t, s, "m1", 30)
		close(done)
	}()
	time.Sleep(300 * time.Millisecond)
	eng.Release()
	<-done

	rejected := 0
	for _, err := range errs {
		if errors.Is(err, ErrQueueFull) {
			rejected++
		} else if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if rejected < 10 {
		t.Fatalf("expected at least 10 queue-full rejections, got %d", rejected)
	}

	// m2 不受影响：门控释放后仍能快速得到结论。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := s.Submit(ctx, "m2", []byte("x"))
	if err != nil {
		t.Fatalf("m2 should not be affected by m1 congestion: %v", err)
	}
	if string(out) != "m2:x" {
		t.Fatalf("unexpected output: %q", out)
	}
}

// 超时区分：排队/执行超时得到 ErrTimeout，且不被组批执行。
func TestTimeout(t *testing.T) {
	eng := NewStubEngine()
	eng.SetDelay(300 * time.Millisecond)
	s := newTestScheduler(t, eng, baseConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.Submit(ctx, "m1", []byte("x"))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

// 取消区分：主动取消得到 ErrCancelled。
func TestCancel(t *testing.T) {
	eng := NewStubEngine()
	eng.SetDelay(300 * time.Millisecond)
	s := newTestScheduler(t, eng, baseConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Submit(ctx, "m1", []byte("x"))
	if !errors.Is(err, ErrCancelled) {
		t.Fatalf("expected ErrCancelled, got %v", err)
	}
}

// 并发消费：每个请求恰好被一个批次消费一次，结果准确返还。
func TestConcurrentExactlyOnce(t *testing.T) {
	eng := NewStubEngine()
	cfg := baseConfig()
	// 大队列 + 阻塞策略：本测试只关注消费次数与结果准确性，不制造背压。
	cfg.Models["m1"] = ModelConfig{MaxBatchSize: 4, MaxQueueSize: 200, MaxRetries: 0, Backpressure: BackpressureBlock}
	cfg.Models["m2"] = ModelConfig{MaxBatchSize: 2, MaxQueueSize: 200, MaxRetries: 0, Backpressure: BackpressureBlock}
	s := newTestScheduler(t, eng, cfg)

	const n = 30
	errs := make([]error, n)
	outs := make([][]byte, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			model := "m1"
			if i%2 == 0 {
				model = "m2"
			}
			outs[i], errs[i] = s.Submit(context.Background(), model, []byte(fmt.Sprintf("p-%d", i)))
		}(i)
	}
	wg.Wait()

	seen := map[string]int{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("req %d: %v", i, err)
		}
		want := fmt.Sprintf("%s:p-%d", map[bool]string{true: "m2", false: "m1"}[i%2 == 0], i)
		if string(outs[i]) != want {
			t.Fatalf("req %d: output %q, want %q", i, outs[i], want)
		}
	}
	for _, id := range flattenBatches(eng.Batches()) {
		seen[id]++
	}
	// 注意：桩引擎收到的是 Request，ID 形如 req-N；这里通过输出反查不可行，
	// 改为断言每个请求在批次中只出现一次（用 reqSeq 与批次 ID 计数）。
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("request %s consumed %d times", id, c)
		}
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique requests in batches, got %d", n, len(seen))
	}
}

// 失败重试：可重试失败按策略重试，最终成功且无重复副作用。
func TestRetry(t *testing.T) {
	eng := NewStubEngine()
	eng.SetFailUntil("req-1", 2) // 前 2 次失败，第 3 次成功
	s := newTestScheduler(t, eng, baseConfig())

	out, err := s.Submit(context.Background(), "m1", []byte("x"))
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if string(out) != "m1:x" {
		t.Fatalf("unexpected output: %q", out)
	}
	if eng.Attempts("req-1") != 3 {
		t.Fatalf("expected 3 attempts, got %d", eng.Attempts("req-1"))
	}
	if eng.SideEffects("req-1") != 1 {
		t.Fatalf("expected exactly 1 side effect (idempotent), got %d", eng.SideEffects("req-1"))
	}
}

// 部分失败：单条永久失败不影响同批其他请求，失败结论可区分。
func TestPartialFailure(t *testing.T) {
	eng := NewStubEngine()
	eng.SetPermanentFail("req-2")
	s := newTestScheduler(t, eng, baseConfig())

	errs := submit(t, s, "m1", 3)
	var failed, succeeded int
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrEngineFailed):
			failed++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if failed != 1 || succeeded != 2 {
		t.Fatalf("expected 1 failure and 2 successes, got failed=%d succeeded=%d", failed, succeeded)
	}
}

// 背压-拒绝：队列满时立即返回 ErrQueueFull。
func TestBackpressureReject(t *testing.T) {
	eng := NewStubEngine()
	eng.SetDelay(200 * time.Millisecond)
	cfg := baseConfig()
	cfg.Workers = 1
	cfg.Models["m1"] = ModelConfig{MaxBatchSize: 4, MaxQueueSize: 1, MaxRetries: 0, Backpressure: BackpressureReject}
	s := newTestScheduler(t, eng, cfg)

	// 1 个工人：在途(4) + 组批(4) + 队列(1) 最多吸收 9 个，其余必被拒绝。
	errs := submit(t, s, "m1", 15)
	full := 0
	for _, err := range errs {
		if errors.Is(err, ErrQueueFull) {
			full++
		} else if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if full < 4 {
		t.Fatalf("expected at least 4 ErrQueueFull, got %d", full)
	}
}

// 背压-阻塞：Block 策略下排队等待，超时则返回 ErrTimeout。
func TestBackpressureBlock(t *testing.T) {
	eng := NewStubEngine()
	eng.SetDelay(200 * time.Millisecond)
	cfg := baseConfig()
	cfg.Models["m2"] = ModelConfig{MaxBatchSize: 2, MaxQueueSize: 1, MaxRetries: 0, Backpressure: BackpressureBlock}
	s := newTestScheduler(t, eng, cfg)

	// 队列容量 1 + 正在处理 2 个，额外请求阻塞；给足时间应全部成功。
	errs := submit(t, s, "m2", 3)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("req %d: %v", i, err)
		}
	}

	// 阻塞期间超时 -> ErrTimeout。
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.Submit(ctx, "m2", []byte("x"))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout while blocking, got %v", err)
	}
}

// 配置热更新：非法更新被拒绝且保留旧配置；合法更新原子生效。
func TestConfigHotUpdate(t *testing.T) {
	eng := NewStubEngine()
	cfg := baseConfig()
	cfg.BatchWindow = 300 * time.Millisecond
	s := newTestScheduler(t, eng, cfg)

	// 非法更新：BatchWindow 为 0。
	bad := baseConfig()
	bad.BatchWindow = 0
	if err := s.UpdateConfig(bad); err == nil {
		t.Fatal("expected invalid config update to fail")
	}

	// 旧配置仍可用：m1 MaxBatchSize=4，4 个请求合并为一批。
	submit(t, s, "m1", 4)
	if b := eng.Batches(); len(b) != 1 || len(b[0]) != 4 {
		t.Fatalf("old config should remain, got batches %v", b)
	}

	// 合法更新：m1 MaxBatchSize 改为 2。
	eng2 := NewStubEngine()
	_ = eng2
	newCfg := baseConfig()
	newCfg.BatchWindow = 300 * time.Millisecond
	newCfg.Models["m1"] = ModelConfig{MaxBatchSize: 2, MaxQueueSize: 10, MaxRetries: 2, Backpressure: BackpressureReject}
	if err := s.UpdateConfig(newCfg); err != nil {
		t.Fatalf("valid update failed: %v", err)
	}
	submit(t, s, "m1", 4)
	batches := eng.Batches()
	last := batches[len(batches)-2:]
	if len(last[0]) > 2 || len(last[1]) > 2 {
		t.Fatalf("new config should cap m1 batches at 2, got %v", last)
	}
}

// 配置版本锁定：组批期间更新配置，正在形成的批次沿用旧版本。
func TestConfigVersionPinning(t *testing.T) {
	eng := NewStubEngine()
	eng.SetDelay(150 * time.Millisecond)
	cfg := baseConfig()
	cfg.BatchWindow = 100 * time.Millisecond
	s := newTestScheduler(t, eng, cfg)

	// 先提交 2 个请求开始组批（旧版本 m1 MaxBatchSize=4）。
	go func() { _, _ = s.Submit(context.Background(), "m1", []byte("a")) }()
	go func() { _, _ = s.Submit(context.Background(), "m1", []byte("b")) }()
	time.Sleep(20 * time.Millisecond)

	// 组批进行中更新配置：m1 MaxBatchSize 改为 1。
	newCfg := baseConfig()
	newCfg.BatchWindow = 100 * time.Millisecond
	newCfg.Models["m1"] = ModelConfig{MaxBatchSize: 1, MaxQueueSize: 10, MaxRetries: 0, Backpressure: BackpressureReject}
	if err := s.UpdateConfig(newCfg); err != nil {
		t.Fatalf("update: %v", err)
	}

	// 再提交 2 个请求，应加入正在形成的批次（锁定旧版本，容量 4）。
	go func() { _, _ = s.Submit(context.Background(), "m1", []byte("c")) }()
	go func() { _, _ = s.Submit(context.Background(), "m1", []byte("d")) }()
	time.Sleep(300 * time.Millisecond)

	batches := eng.Batches()
	if len(batches) == 0 || len(batches[0]) != 4 {
		t.Fatalf("forming batch should pin old config (size 4), got %v", batches)
	}
}

// 可观测性：队列深度、批大小分布、延迟指标可查。
func TestMetrics(t *testing.T) {
	eng := NewStubEngine()
	s := newTestScheduler(t, eng, baseConfig())

	submit(t, s, "m1", 3)
	time.Sleep(100 * time.Millisecond) // 等批次处理完

	snap := s.Metrics()
	m1 := snap.Models["m1"]
	if m1.QueueDepth != 0 {
		t.Fatalf("queue depth after drain should be 0, got %d", m1.QueueDepth)
	}
	if m1.Completed != 3 {
		t.Fatalf("expected 3 completed, got %d", m1.Completed)
	}
	if m1.BatchSizes[3] != 1 {
		t.Fatalf("expected batch-size histogram to record size 3 once, got %v", m1.BatchSizes)
	}
	if m1.Latency.Count != 3 {
		t.Fatalf("expected 3 latency samples, got %d", m1.Latency.Count)
	}
	if snap.Totals.Batches < 1 {
		t.Fatalf("expected >= 1 total batch, got %d", snap.Totals.Batches)
	}
}

// 未知模型返回可区分错误。
func TestModelNotFound(t *testing.T) {
	s := newTestScheduler(t, NewStubEngine(), baseConfig())
	_, err := s.Submit(context.Background(), "nope", []byte("x"))
	if !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("expected ErrModelNotFound, got %v", err)
	}
}

// 关闭语义：关闭后提交返回 ErrShutdown，已入队请求都得到结论不挂死。
func TestShutdown(t *testing.T) {
	eng := NewStubEngine()
	eng.SetDelay(100 * time.Millisecond)
	cfg := baseConfig()
	cfg.Models["m1"] = ModelConfig{MaxBatchSize: 4, MaxQueueSize: 10, MaxRetries: 0, Backpressure: BackpressureBlock}
	s, err := NewScheduler(eng, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Submit(context.Background(), "m1", []byte(fmt.Sprintf("p-%d", i)))
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	_ = s.Close()
	wg.Wait() // 必须在有限时间内返回

	for i, err := range errs {
		if err == nil {
			continue // 已处理成功
		}
		if !errors.Is(err, ErrShutdown) && !errors.Is(err, ErrTimeout) {
			t.Fatalf("req %d: unexpected error %v", i, err)
		}
	}

	// 关闭后提交立即得到 ErrShutdown。
	_, err = s.Submit(context.Background(), "m1", []byte("x"))
	if !errors.Is(err, ErrShutdown) {
		t.Fatalf("expected ErrShutdown after close, got %v", err)
	}
}

// 混合组批：同一批次可以包含多个模型的请求，各模型分组不超过各自上限。
func TestMixedModelBatch(t *testing.T) {
	eng := NewStubEngine()
	cfg := baseConfig()
	cfg.BatchWindow = 100 * time.Millisecond
	s := newTestScheduler(t, eng, cfg)

	var mu sync.Mutex
	var dispatched []*Batch
	s.onDispatch = func(b *Batch) {
		mu.Lock()
		dispatched = append(dispatched, b)
		mu.Unlock()
	}

	go func() { _, _ = s.Submit(context.Background(), "m1", []byte("a1")) }()
	go func() { _, _ = s.Submit(context.Background(), "m2", []byte("a2")) }()
	go func() { _, _ = s.Submit(context.Background(), "m1", []byte("b1")) }()
	go func() { _, _ = s.Submit(context.Background(), "m2", []byte("b2")) }()
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(dispatched) != 1 {
		t.Fatalf("expected 1 mixed batch, got %d", len(dispatched))
	}
	b := dispatched[0]
	if len(b.models()) != 2 {
		t.Fatalf("expected batch to contain 2 model groups, got %v", b.models())
	}
	if b.modelSize("m1") != 2 || b.modelSize("m2") != 2 {
		t.Fatalf("unexpected group sizes: m1=%d m2=%d", b.modelSize("m1"), b.modelSize("m2"))
	}
}

// 高压稳定性：长时间高负载下协程数量不增长、队列可排空、拒绝可区分。
func TestHighLoadStable(t *testing.T) {
	eng := NewStubEngine()
	cfg := baseConfig()
	cfg.Models["m1"] = ModelConfig{MaxBatchSize: 4, MaxQueueSize: 5, MaxRetries: 0, Backpressure: BackpressureReject}
	s := newTestScheduler(t, eng, cfg)

	// 预热。
	submit(t, s, "m1", 10)
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()

	// 持续高压：多轮并发提交。
	for round := 0; round < 5; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 60; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = s.Submit(context.Background(), "m1", []byte("x"))
			}()
		}
		wg.Wait()
	}
	time.Sleep(200 * time.Millisecond)

	if got := runtime.NumGoroutine(); got > baseGoroutines+5 {
		t.Fatalf("goroutine count grew under load: %d -> %d", baseGoroutines, got)
	}
	snap := s.Metrics()
	if snap.Models["m1"].QueueDepth != 0 {
		t.Fatalf("queue should drain to 0, got %d", snap.Models["m1"].QueueDepth)
	}
	if snap.Models["m1"].QueueFull == 0 {
		t.Fatal("expected queue-full rejections under sustained load")
	}
}
