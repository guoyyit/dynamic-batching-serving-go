package serving

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestRejectBackpressure 验证队列达到上界时按 reject 策略拒绝，
// 拒绝原因为 queue_full，且拒绝数可观测。
func TestRejectBackpressure(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.QueueCapacity = 3
	mc.MaxBatchSize = 100
	mc.BatchWindow = 2 * time.Second // 让请求稳定停留在队列中
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ok, full := 0, 0
	for i := 0; i < 10; i++ {
		_, err := s.SubmitChan(context.Background(), Request{Model: "m", Payload: i})
		if err != nil {
			if reasonOf(err) != ReasonQueueFull {
				t.Fatalf("want queue_full, got %v", err)
			}
			full++
		} else {
			ok++
		}
	}
	if ok != 3 || full != 7 {
		t.Fatalf("want 3 accepted / 7 rejected, got %d / %d", ok, full)
	}
	st := s.Stats()
	for _, ms := range st.Models {
		if ms.Model == "m" {
			if ms.QueueDepth != 3 {
				t.Fatalf("queue depth must stay at cap, got %d", ms.QueueDepth)
			}
			if ms.Rejected != 7 {
				t.Fatalf("want 7 rejected metric, got %d", ms.Rejected)
			}
		}
	}
}

// TestBlockBackpressureBounded 验证 block 策略下高负载时：
// 在队深度不超过容量，全部请求最终成功，调用方 goroutine 之外
// 不产生无界资源（这里通过结果完整性间接验证内存/协程不泄漏）。
func TestBlockBackpressureBounded(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.QueueCapacity = 4
	mc.MaxBatchSize = 2
	mc.BatchWindow = 2 * time.Millisecond
	mc.OverflowPolicy = OverflowBlock
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 120
	var wg sync.WaitGroup
	var mx sync.Mutex
	maxDepth := 0
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			out, err := s.Submit(context.Background(), Request{Model: "m", Payload: i})
			if err != nil {
				t.Errorf("req %d: %v", i, err)
				return
			}
			if out.(EchoOutput).Value.(int) != i {
				t.Errorf("mismatch at %d", i)
			}
			qd, outstanding, qcap := 0, 0, 0
			for _, ms := range s.Stats().Models {
				if ms.Model == "m" {
					qd, outstanding, qcap = ms.QueueDepth, ms.Outstanding, ms.QueueCap
				}
			}
			mx.Lock()
			if qd > maxDepth {
				maxDepth = qd
			}
			if outstanding > qcap {
				mx.Unlock()
				t.Errorf("outstanding %d exceeded cap %d", outstanding, qcap)
				return
			}
			mx.Unlock()
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("blocked load did not finish")
	}
	if maxDepth > 4 {
		t.Fatalf("queue depth exceeded cap: %d", maxDepth)
	}
}

// TestRejectDistinctFromNotFound 验证队列满与模型不存在原因不同。
func TestRejectDistinctFromNotFound(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.QueueCapacity = 1
	mc.MaxBatchSize = 100
	mc.BatchWindow = 2 * time.Second
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, e1 := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 1})
	if e1 != nil {
		t.Fatal(e1)
	}
	_, e2 := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 2})
	if reasonOf(e2) != ReasonQueueFull {
		t.Fatalf("want queue_full, got %v", e2)
	}
	_, e3 := s.SubmitChan(context.Background(), Request{Model: "other", Payload: 1})
	if reasonOf(e3) != ReasonModelNotFound {
		t.Fatalf("want model_not_found, got %v", e3)
	}
}

// TestCloseRejectsAndSettles 验证关闭语义：
// Close 后提交得到 scheduler_closed/queue_closed；在队请求被结算而非丢弃。
func TestCloseRejectsAndSettles(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 100
	mc.BatchWindow = 5 * time.Second
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	select {
	case r := <-ch:
		if r.Err == nil {
			t.Fatal("queued request should get a close conclusion")
		}
	case <-time.After(time.Second):
		t.Fatal("queued request dropped on close")
	}
	_, err = s.Submit(context.Background(), Request{Model: "m", Payload: 2})
	if reasonOf(err) != ReasonSchedulerClosed && reasonOf(err) != ReasonQueueClosed {
		t.Fatalf("want closed reason, got %v", err)
	}
}
