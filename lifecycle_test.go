package serving

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeak 验证 Close 后调度循环与工作协程全部退出。
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		engine := NewStubEngine()
		mc := baseModel("m")
		s, err := New(testConfig(int64(i+1), mc), engine)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 10; j++ {
			_, _ = s.Submit(context.Background(), Request{Model: "m", Payload: j})
		}
		s.Close()
		s.Close() // 重复关闭必须安全
	}
	// 给运行时一点时间回收已退出的 goroutine 栈。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before+1 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine leak: before=%d after=%d\n%s", before, got, buf[:n])
	}
}

// TestStatsLatencyPercentiles 验证批延迟采样与百分位被正确填充。
func TestStatsLatencyPercentiles(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.BatchWindow = time.Millisecond
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 20; i++ {
		_, _ = s.Submit(context.Background(), Request{Model: "m", Payload: i})
	}
	deadline := time.Now().Add(2 * time.Second)
	var st SchedulerStats
	for time.Now().Before(deadline) {
		st = s.Stats()
		if st.BatchesSent >= 20 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st.BatchesSent != 20 {
		t.Fatalf("want 20 batches, got %d", st.BatchesSent)
	}
	if len(st.BatchLatencyMicros) != 20 {
		t.Fatalf("want 20 latency samples, got %d", len(st.BatchLatencyMicros))
	}
	p50 := st.BatchLatencyPercentiles["p50"]
	p99 := st.BatchLatencyPercentiles["p99"]
	if p50 < 0 || p99 < 0 || p50 > p99 {
		t.Fatalf("bad percentiles p50=%d p99=%d", p50, p99)
	}
}

// TestStatsQueueDepthObservable 验证各模型队列深度独立可观测。
func TestStatsQueueDepthObservable(t *testing.T) {
	engine := NewStubEngine()
	a := baseModel("a")
	a.MaxBatchSize = 100
	a.BatchWindow = 5 * time.Second
	b := baseModel("b")
	b.MaxBatchSize = 100
	b.BatchWindow = 5 * time.Second
	s, err := New(testConfig(1, a, b), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, _ = s.SubmitChan(context.Background(), Request{Model: "a", Payload: 1})
	_, _ = s.SubmitChan(context.Background(), Request{Model: "a", Payload: 2})
	_, _ = s.SubmitChan(context.Background(), Request{Model: "b", Payload: 1})

	depths := map[string]int{}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, ms := range s.Stats().Models {
			depths[ms.Model] = ms.QueueDepth
		}
		if depths["a"] == 2 && depths["b"] == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected depths a=2 b=1, got %v", depths)
}
