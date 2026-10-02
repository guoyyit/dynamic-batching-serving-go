package serving

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestHotUpdateInvalidKeepsOld 验证非法/旧版本更新被拒绝，
// 上一个可用配置继续生效。
func TestHotUpdateInvalidKeepsOld(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	s, err := New(testConfig(7, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 旧版本号：拒绝。
	bad := testConfig(3, mc)
	if err := s.UpdateConfig(bad); err == nil || reasonOf(err) != ReasonInvalidConfig {
		t.Fatalf("stale version must be rejected, got %v", err)
	}
	// 非法字段：拒绝。
	bad2 := testConfig(8, mc)
	bad2.Models["m"] = mc
	mc2 := bad2.Models["m"]
	mc2.MaxBatchSize = -1
	bad2.Models["m"] = mc2
	if err := s.UpdateConfig(bad2); err == nil {
		t.Fatal("negative field must be rejected")
	}
	if v := s.CurrentVersion(); v != 7 {
		t.Fatalf("old config must remain at version 7, got %d", v)
	}
}

// TestHotUpdateAddRemoveModel 验证运行时新增/移除模型：
// 新模型立即可用；移除后其在队请求以 queue_closed 结算，
// 新提交得到 model_not_found。
func TestHotUpdateAddRemoveModel(t *testing.T) {
	engine := NewStubEngine()
	a := baseModel("a")
	a.MaxBatchSize = 100
	a.BatchWindow = 5 * time.Second // 让请求停在队列
	s, err := New(testConfig(1, a), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ch, err := s.SubmitChan(context.Background(), Request{Model: "a", Payload: 1})
	if err != nil {
		t.Fatal(err)
	}

	// 版本 2：移除 a，新增 b。
	b := baseModel("b")
	if err := s.UpdateConfig(testConfig(2, b)); err != nil {
		t.Fatal(err)
	}
	if v := s.CurrentVersion(); v != 2 {
		t.Fatalf("want version 2, got %d", v)
	}
	select {
	case r := <-ch:
		if reasonOf(r.Err) != ReasonQueueClosed {
			t.Fatalf("removed model queued req want queue_closed, got %v", r.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("removed model request not settled")
	}
	if _, err := s.Submit(context.Background(), Request{Model: "a", Payload: 1}); reasonOf(err) != ReasonModelNotFound {
		t.Fatalf("removed model want model_not_found, got %v", err)
	}
	cb, err := s.SubmitChan(context.Background(), Request{Model: "b", Payload: "new"})
	if err != nil {
		t.Fatalf("new model should work: %v", err)
	}
	if r := waitResult(t, cb); r.Err != nil {
		t.Fatalf("new model req: %v", r.Err)
	}
}

// TestHotUpdateGrowShrinkCapacity 验证容量热更新：
// 扩缩容保留 FIFO 在队请求；缩容到小于当前深度被拒绝。
func TestHotUpdateGrowShrinkCapacity(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.QueueCapacity = 4
	mc.MaxBatchSize = 100
	mc.BatchWindow = 5 * time.Second
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	chs := make([]<-chan Response, 4)
	for i := range chs {
		chs[i], _ = s.SubmitChan(context.Background(), Request{Model: "m", Payload: i})
	}
	// 缩容到 2（当前深度 4）：必须拒绝且保留旧容量。
	mc2 := mc
	mc2.QueueCapacity = 2
	if err := s.UpdateConfig(testConfig(2, mc2)); err == nil {
		t.Fatal("shrink below current depth must be rejected")
	}
	var capNow int
	for _, ms := range s.Stats().Models {
		if ms.Model == "m" {
			capNow = ms.QueueCap
		}
	}
	if capNow != 4 {
		t.Fatalf("old capacity must remain 4, got %d", capNow)
	}

	// 扩容到 8：成功。
	mc3 := mc
	mc3.QueueCapacity = 8
	if err := s.UpdateConfig(testConfig(3, mc3)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 4}); err != nil {
		t.Fatalf("after grow, slot 5 must be accepted: %v", err)
	}
}

// TestHotUpdateBatchParamsTakeEffect 验证批次参数热更新对后续组批生效。
func TestHotUpdateBatchParamsTakeEffect(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 100
	mc.BatchWindow = 2 * time.Second
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ch, _ := s.SubmitChan(context.Background(), Request{Model: "m", Payload: 1})
	// 初始窗口很长，不应被处理。
	time.Sleep(40 * time.Millisecond)
	if engine.CallCount("m") != 0 {
		t.Fatal("must not batch before window")
	}
	// 热更新把窗口缩到很短，后续组批应使用新版本。
	mc2 := mc
	mc2.BatchWindow = 5 * time.Millisecond
	if err := s.UpdateConfig(testConfig(2, mc2)); err != nil {
		t.Fatal(err)
	}
	if r := waitResult(t, ch); r.Err != nil {
		t.Fatalf("after window shrink, request should complete: %v", r.Err)
	}
}

// TestConcurrentBatchingAndUpdate 验证“组批与更新并发”时，
// 正在进行的批次落在某个完整版本，请求不丢不重。
func TestConcurrentBatchingAndUpdate(t *testing.T) {
	engine := NewStubEngine()
	mc := baseModel("m")
	mc.MaxBatchSize = 8
	mc.BatchWindow = 3 * time.Millisecond
	mc.OverflowPolicy = OverflowBlock
	s, err := New(testConfig(1, mc), engine)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stop := make(chan struct{})
	upDone := make(chan struct{})
	go func() {
		defer close(upDone)
		v := int64(2)
		for {
			select {
			case <-stop:
				return
			default:
			}
			next := testConfig(v, mc)
			// 更新可能因版本/瞬时深度失败，忽略错误（旧版本继续可用）。
			_ = s.UpdateConfig(next)
			v++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	const n = 300
	results := make([]int, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := s.Submit(context.Background(), Request{Model: "m", Payload: i})
			errs[i] = err
			if err == nil {
				results[i] = out.(EchoOutput).Value.(int)
			}
		}(i)
	}
	wgDone := make(chan struct{})
	go func() { wg.Wait(); close(wgDone) }()
	select {
	case <-wgDone:
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent batching+update deadlocked")
	}
	close(stop)
	<-upDone

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("req %d unexpected err: %v", i, errs[i])
		}
		if results[i] != i {
			t.Fatalf("mismatch %d -> %d", i, results[i])
		}
	}
	var completed int64
	for _, ms := range s.Stats().Models {
		if ms.Model == "m" {
			completed = ms.Completed
		}
	}
	if completed != n {
		t.Fatalf("want %d completed, got %d", n, completed)
	}
}

// TestConfigValidate 覆盖若干非法配置分支。
func TestConfigValidate(t *testing.T) {
	if err := (*Config)(nil).Validate(); err == nil {
		t.Fatal("nil config must fail")
	}
	mc := baseModel("m")
	c := testConfig(1, mc)
	c.Version = 0
	if err := c.Validate(); reasonOf(err) != ReasonInvalidConfig {
		t.Fatalf("zero version, got %v", err)
	}
	c = testConfig(1, mc)
	c.Global.NumWorkers = -1
	if err := c.Validate(); reasonOf(err) != ReasonInvalidConfig {
		t.Fatalf("negative workers, got %v", err)
	}
	c = testConfig(1, mc)
	bad := mc
	bad.RetryStrategy = "bogus"
	c.Models["m"] = bad
	if err := c.Validate(); reasonOf(err) != ReasonInvalidConfig {
		t.Fatalf("bad enum, got %v", err)
	}
	c = testConfig(1, mc)
	bad2 := mc
	bad2.MaxRetries = 2
	bad2.RetryStrategy = RetryNever
	c.Models["m"] = bad2
	if err := c.Validate(); err == nil {
		t.Fatal("retries with never strategy must fail")
	}
}
