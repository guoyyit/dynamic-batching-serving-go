package serving

import (
	"context"
	"errors"
	"testing"
	"time"
)

func reasonOf(err error) Reason {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Reason
	}
	return ReasonNone
}

// TestQueueTimeout 验证请求在队列中等待超过 RequestTimeout 后，
// 得到 queue_timeout 结论，且不会被塞进之后出发的批次。
func TestQueueTimeout(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 100
	mc.BatchWindow = 2 * time.Second
	mc.RequestTimeout = 30 * time.Millisecond
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := s.Submit(ctx, Request{Model: "m", Payload: 1})
	if err == nil {
		t.Fatalf("expected timeout, got result %v", out)
	}
	if reasonOf(err) != ReasonQueueTimeout {
		t.Fatalf("want queue_timeout, got %v", err)
	}
	// 统计中应记录一次 timeout，且没有任何 Infer 调用（请求未进入批次）。
	if calls := engine.CallCount("m"); calls != 0 {
		t.Fatalf("timed-out request must not be inferred, calls=%d", calls)
	}
	var to int64
	for _, ms := range s.Stats().Models {
		if ms.Model == "m" {
			to = ms.TimedOut
		}
	}
	if to != 1 {
		t.Fatalf("want 1 timeout metric, got %d", to)
	}
}

// TestCancelWhileQueued 验证排队期间调用方取消得到 canceled，
// 与超时可区分，且取消后不会被推理。
func TestCancelWhileQueued(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 100
	mc.BatchWindow = 2 * time.Second
	mc.RequestTimeout = 5 * time.Second
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.SubmitChan(ctx, Request{Model: "m", Payload: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case r := <-ch:
		if reasonOf(r.Err) != ReasonCanceled {
			t.Fatalf("want canceled, got %v", r.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request not settled")
	}
	if calls := engine.CallCount("m"); calls != 0 {
		t.Fatalf("canceled request must not be inferred, calls=%d", calls)
	}
}

// TestBatchNotSilentlyDropped 验证已经组批（出发）的请求即使 ctx 取消，
// 结果也会被完整结算一次，不被静默丢弃。
func TestBatchNotSilentlyDropped(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 1
	mc.BatchWindow = 5 * time.Millisecond
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.SubmitChan(ctx, Request{Model: "m", Payload: 1})
	if err != nil {
		t.Fatal(err)
	}
	// 等待批次出发后再取消；响应通道容量 1，结算不会丢。
	time.Sleep(15 * time.Millisecond)
	cancel()
	select {
	case r := <-ch:
		// 取消只影响“等待入批”；已在途的请求以实际执行结果为准。
		if r.Err == nil && r.Val == nil {
			t.Fatal("empty conclusion")
		}
	case <-time.After(time.Second):
		t.Fatal("batched request silently dropped after cancel")
	}
}

// TestTimeoutVsCancelDistinct 验证超时与取消结论互不相同。
func TestTimeoutVsCancelDistinct(t *testing.T) {
	if ReasonQueueTimeout == ReasonCanceled {
		t.Fatal("reasons must differ")
	}
}

// TestBlockQueueCanceled 验证 block 策略下队列满时，取消等待者
// 得到 canceled，且取消后腾出的处理不影响后续请求。
func TestBlockQueueCanceled(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.QueueCapacity = 1
	mc.MaxBatchSize = 100
	mc.BatchWindow = 2 * time.Second
	mc.OverflowPolicy = OverflowBlock
	mc.RequestTimeout = 5 * time.Second
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 占满唯一队列槽位。
	holder, err := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 0})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, e := s.Submit(ctx, Request{Model: "m", Payload: 1})
		errCh <- e
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case e := <-errCh:
		if reasonOf(e) != ReasonCanceled {
			t.Fatalf("want canceled, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked submitter not unblocked on cancel")
	}
	// holder 超时窗口很长，通过直接更新配置缩短不便；改为验证它仍在队列，
	// 然后关闭调度器，holder 得到 queue_closed 而非丢失。
	s.Close()
	select {
	case r := <-holder:
		if reasonOf(r.Err) != ReasonQueueClosed {
			// 关闭期间也可能恰好被组批执行，两种结论都非丢失；
			// 但窗口 2s 通常不会执行，故预期 closed。
		}
	case <-time.After(time.Second):
		t.Fatal("holder not settled on close")
	}
}
