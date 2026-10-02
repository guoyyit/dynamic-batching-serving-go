package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// modelState 单个模型的运行时状态：独立队列通道。
type modelState struct {
	name string
	ch   chan *Request
}

// Scheduler 是本地推理调度器：多模型共享同一入口，各自拥有独立队列与批处理参数。
type Scheduler struct {
	engine Engine

	cfg     atomic.Pointer[configVersion]
	version atomic.Int64
	batchID atomic.Int64
	reqSeq  atomic.Int64

	modelsMu sync.RWMutex
	models   map[string]*modelState

	// arrivals 各模型队列汇总后的请求入口（batcher 消费）。
	arrivals chan *Request
	// batches 组批完成、发往工作协程的批次通道。
	batches chan *Batch

	stop      chan struct{}
	stopOnce  sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup // feeders + batcher
	workersWg sync.WaitGroup // workers

	metrics *metricsRegistry

	// onDispatch 批次派发回调（仅用于测试观测组批结果），可为 nil。
	onDispatch func(b *Batch)
}

// NewScheduler 创建调度器并启动组批协程与工作协程。
func NewScheduler(engine Engine, cfg Config) (*Scheduler, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Scheduler{
		engine:   engine,
		models:   map[string]*modelState{},
		arrivals: make(chan *Request),
		batches:  make(chan *Batch),
		stop:     make(chan struct{}),
		metrics:  newMetricsRegistry(),
	}
	s.version.Store(1)
	s.cfg.Store(&configVersion{version: 1, cfg: cfg})

	for name, mc := range cfg.Models {
		st := &modelState{name: name, ch: make(chan *Request, mc.MaxQueueSize)}
		s.models[name] = st
		s.wg.Add(1)
		go s.runFeeder(st)
	}

	s.wg.Add(1)
	go s.runBatcher()

	for i := 0; i < cfg.Workers; i++ {
		s.workersWg.Add(1)
		go s.runWorker()
	}
	return s, nil
}

// Submit 提交一个推理请求，阻塞直到拿到结论或上下文结束。
func (s *Scheduler) Submit(ctx context.Context, model string, payload []byte) ([]byte, error) {
	select {
	case <-s.stop:
		return nil, ErrShutdown
	default:
	}

	cfg := s.cfg.Load()
	mc, ok := cfg.cfg.Models[model]
	if !ok {
		return nil, ErrModelNotFound
	}
	st := s.getState(model)
	if st == nil {
		return nil, ErrModelNotFound
	}

	r := &Request{
		ID:          fmt.Sprintf("req-%d", s.reqSeq.Add(1)),
		Model:       model,
		Payload:     payload,
		ctx:         ctx,
		done:        make(chan Outcome, 1),
		submittedAt: time.Now(),
	}

	// 入队：先尝试无阻塞发送。
	select {
	case st.ch <- r:
	default:
		if mc.Backpressure == BackpressureReject {
			s.metrics.recordQueueFull(model)
			return nil, ErrQueueFull
		}
		// Block 策略：阻塞等待队列空间，同时响应超时/取消/关闭。
		select {
		case st.ch <- r:
		case <-ctx.Done():
			return nil, mapCtxErr(ctx.Err())
		case <-s.stop:
			return nil, ErrShutdown
		}
	}
	s.metrics.recordSubmitted(model)

	select {
	case o := <-r.done:
		return o.Output, o.Err
	case <-ctx.Done():
		return nil, mapCtxErr(ctx.Err())
	case <-s.stop:
		return nil, ErrShutdown
	}
}

// UpdateConfig 热更新配置：校验失败返回错误且保留上一个可用配置；
// 校验通过后原子切换，正在组批的批次沿用切换前锁定的版本。
func (s *Scheduler) UpdateConfig(cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	s.modelsMu.Lock()
	defer s.modelsMu.Unlock()

	select {
	case <-s.stop:
		return ErrShutdown
	default:
	}

	// 移除模型前必须确认其队列已空，避免请求被静默丢弃。
	for name, st := range s.models {
		if _, ok := cfg.Models[name]; !ok {
			if n := len(st.ch); n > 0 {
				return fmt.Errorf("sched: cannot remove model %q: %d request(s) still queued", name, n)
			}
		}
	}

	next := make(map[string]*modelState, len(cfg.Models))
	for name := range cfg.Models {
		if st, ok := s.models[name]; ok {
			next[name] = st
			continue
		}
		st := &modelState{name: name, ch: make(chan *Request, cfg.Models[name].MaxQueueSize)}
		next[name] = st
		s.wg.Add(1)
		go s.runFeeder(st)
	}
	for name, st := range s.models {
		if _, ok := next[name]; !ok {
			close(st.ch)
		}
	}
	s.models = next

	v := s.version.Add(1)
	s.cfg.Store(&configVersion{version: v, cfg: cfg})
	return nil
}

// Metrics 返回当前指标快照。
func (s *Scheduler) Metrics() MetricsSnapshot {
	return s.metrics.snapshot(func(model string) int {
		st := s.getState(model)
		if st == nil {
			return 0
		}
		return len(st.ch)
	})
}

// Close 关闭调度器：停止接收新请求；已入队/已组批的请求都会得到可区分结论。
// 重复调用安全。
func (s *Scheduler) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	// 与 UpdateConfig 互斥，避免“Wait 返回后又 Add”的竞态。
	s.modelsMu.Lock()
	s.wg.Wait()
	s.modelsMu.Unlock()
	s.closeOnce.Do(func() { close(s.batches) })
	s.workersWg.Wait() // 批次通道关闭后，worker 处理完剩余批次退出
	return nil
}

// runFeeder 把单个模型队列的请求汇总到 arrivals；关闭时排空队列，
// 保证每个已入队请求都得到结论（ErrShutdown），不静默丢弃。
func (s *Scheduler) runFeeder(st *modelState) {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			s.drainModelQueue(st)
			return
		case r, ok := <-st.ch:
			if !ok {
				return
			}
			select {
			case s.arrivals <- r:
			case <-s.stop:
				s.finish(r, Outcome{Err: ErrShutdown})
				s.drainModelQueue(st)
				return
			}
		}
	}
}

// drainModelQueue 非阻塞排空模型队列，所有请求以 ErrShutdown 结论收尾。
func (s *Scheduler) drainModelQueue(st *modelState) {
	for {
		select {
		case r, ok := <-st.ch:
			if !ok {
				return
			}
			s.finish(r, Outcome{Err: ErrShutdown})
		default:
			return
		}
	}
}

// runBatcher 组批循环：按批窗口与队列深度动态组批。
// 触发条件（任一）：
//  1. 批次从首个请求到达起经过 BatchWindow（批窗口）；
//  2. 批次总请求数达到全局 MaxBatchSize（队列深度）；
//  3. 某模型分组达到该模型 MaxBatchSize（发往该模型的批次不得超限）。
func (s *Scheduler) runBatcher() {
	defer s.wg.Done()

	var forming *Batch
	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		timerC = nil
	}

	for {
		select {
		case <-s.stop:
			s.failForming(forming)
			stopTimer()
			return
		case r := <-s.arrivals:
			// 排队期间已超时/取消的请求不再组批，给出可区分结论。
			if !r.alive() {
				s.finish(r, Outcome{Err: mapCtxErr(r.ctx.Err())})
				continue
			}
			if forming == nil {
				forming = newBatch(s.cfg.Load(), s.nextBatchID())
				timer = time.NewTimer(forming.cfg.cfg.BatchWindow)
				timerC = timer.C
			}
			mc := forming.cfg.cfg.Models[r.Model]
			// 该模型分组将超限：先发走当前批次（溢出请求随下一批，FIFO 不乱序）。
			if forming.modelSize(r.Model) >= mc.MaxBatchSize {
				s.dispatch(forming)
				forming = newBatch(s.cfg.Load(), s.nextBatchID())
				timer.Stop()
				timer = time.NewTimer(forming.cfg.cfg.BatchWindow)
				timerC = timer.C
			}
			forming.add(r)
			if g := forming.cfg.cfg.MaxBatchSize; g > 0 && forming.size >= g {
				s.dispatch(forming)
				forming = nil
				stopTimer()
			}
		case <-timerC:
			timerC = nil
			if forming != nil && forming.size > 0 {
				s.dispatch(forming)
				forming = nil
			}
		}
	}
}

// dispatch 把批次发往工作协程；关闭时批次不得静默丢弃，逐个给出 ErrShutdown。
func (s *Scheduler) dispatch(b *Batch) {
	select {
	case s.batches <- b:
		if s.onDispatch != nil {
			s.onDispatch(b)
		}
	case <-s.stop:
		s.failForming(b)
	}
}

// runWorker 消费批次并执行推理，多个工作协程并发消费。
func (s *Scheduler) runWorker() {
	defer s.workersWg.Done()
	for b := range s.batches {
		s.processBatch(b)
	}
}

// processBatch 处理一个批次：按模型分组（确定顺序）分别执行；
// 失败仅重试其中可重试的成员（部分重试），成功成员不重复执行；
// 每个请求最终都恰好得到一条完整结论。
func (s *Scheduler) processBatch(b *Batch) {
	groupSizes := map[string]int{}

	for _, model := range b.models() {
		items := b.group(model)
		groupSizes[model] = len(items)
		mc := b.cfg.cfg.Models[model]

		pending := items
		for attempt := 0; attempt <= mc.MaxRetries; attempt++ {
			live := make([]*Request, 0, len(pending))
			for _, r := range pending {
				if !r.alive() {
					s.finish(r, Outcome{Err: mapCtxErr(r.ctx.Err())})
					continue
				}
				live = append(live, r)
			}
			if len(live) == 0 {
				break
			}

			results := s.engine.Infer(model, live)
			resByID := make(map[string]*ItemResult, len(results))
			for _, res := range results {
				resByID[res.ReqID] = res
			}

			next := make([]*Request, 0)
			for _, r := range live {
				res := resByID[r.ID]
				if res == nil {
					// 引擎漏返回结论时按可重试失败处理，避免静默丢失。
					res = &ItemResult{ReqID: r.ID, Err: ErrEngineFailed, Retryable: true}
				}
				if res.Err == nil {
					s.finish(r, Outcome{Output: res.Output})
					continue
				}
				if res.Retryable && attempt < mc.MaxRetries {
					next = append(next, r)
					continue
				}
				s.finish(r, Outcome{Err: fmt.Errorf("%w: %v", ErrEngineFailed, res.Err)})
			}
			pending = next
			if len(pending) == 0 {
				break
			}

			s.metrics.recordRetry(model)
			if mc.RetryBackoff > 0 {
				t := time.NewTimer(mc.RetryBackoff)
				select {
				case <-t.C:
				case <-s.stop:
					t.Stop()
					for _, r := range pending {
						s.finish(r, Outcome{Err: ErrShutdown})
					}
					pending = nil
				}
			}
		}

		// 重试耗尽仍未成功者，给出明确的失败结论。
		for _, r := range pending {
			if r.alive() {
				s.finish(r, Outcome{Err: fmt.Errorf("%w: retries exhausted", ErrEngineFailed)})
			} else {
				s.finish(r, Outcome{Err: mapCtxErr(r.ctx.Err())})
			}
		}
	}

	s.metrics.recordBatch(b.size, groupSizes)
}

// failForming 关闭时对批次内所有请求给出 ErrShutdown 结论。
func (s *Scheduler) failForming(b *Batch) {
	if b == nil {
		return
	}
	for _, model := range b.models() {
		for _, r := range b.group(model) {
			s.finish(r, Outcome{Err: ErrShutdown})
		}
	}
}

// finish 恰好一次地返还结论并记录指标。
func (s *Scheduler) finish(r *Request, o Outcome) {
	if r.finish(o) {
		s.metrics.recordOutcome(r.Model, o, float64(time.Since(r.submittedAt).Microseconds())/1000.0)
	}
}

// nextBatchID 返回递增的批次编号。
func (s *Scheduler) nextBatchID() int64 {
	return s.batchID.Add(1)
}

// getState 读取模型运行时状态。
func (s *Scheduler) getState(model string) *modelState {
	s.modelsMu.RLock()
	defer s.modelsMu.RUnlock()
	return s.models[model]
}

// mapCtxErr 把上下文错误映射为可区分的调度错误。
func mapCtxErr(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return ErrTimeout
	case errors.Is(err, context.Canceled):
		return ErrCancelled
	default:
		return err
	}
}
