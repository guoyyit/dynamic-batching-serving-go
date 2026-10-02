package serving

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// 调度时间分辨率：用于精确判定 BatchWindow 是否到期的细粒度节拍。
// 基础节拍 TickInterval 控制每个模型每轮的最大出队扫描频率；
// 深度触发（达到 MaxBatchSize）由提交/重试动作立即唤醒，不等待节拍。
const windowGranularity = time.Millisecond

// Scheduler 是多模型共用的本地动态批处理调度入口。
//
// 资源模型（保证有界）：
//   - 每个模型独立有界队列（QueueCapacity），互不挤占；
//   - 工作协程数量固定（GlobalConfig.NumWorkers），不随负载增长；
//   - 调度循环只有 1 个常驻 goroutine；Submit 阻塞时等待者数量
//     等于调用方自身 goroutine，在队条目数永远不超过队列容量上界。
type Scheduler struct {
	engine  Inferencer
	reg     *registry
	metrics *metrics

	stateMu   sync.Mutex
	inFlight  int
	closed    bool
	startTime time.Time

	batches chan *batch
	workers *workerPool
	nextID  int64

	stopCh chan struct{}
	loopWg sync.WaitGroup
	wakeCh chan struct{}
}

// New 创建调度器，校验配置并启动调度循环与固定数量工作协程。
func New(cfg Config, engine Inferencer) (*Scheduler, error) {
	if engine == nil {
		return nil, apiErr(ReasonInvalidConfig, "engine is nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := cfg.clone()
	c.normalize()

	s := &Scheduler{
		engine:    engine,
		reg:       newRegistry(c),
		metrics:   newMetrics(),
		batches:   make(chan *batch, c.Global.NumWorkers),
		stopCh:    make(chan struct{}),
		wakeCh:    make(chan struct{}, 1),
		startTime: time.Now(),
	}
	s.workers = newWorkerPool(c.Global.NumWorkers, engine, s.batches, s.metrics, s)
	s.workers.start()
	s.loopWg.Add(1)
	go s.runLoop()
	return s, nil
}

// Submit 同步提交一个推理请求：按模型策略排队，阻塞直到得到结论、
// ctx 取消或请求超时。返回的 error 若为 *APIError，其 Reason 可区分
// 模型不存在、队列满、超时、取消、执行失败、重试耗尽等情况。
func (s *Scheduler) Submit(ctx context.Context, req Request) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ch, err := s.submit(ctx, req)
	if err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		return resp.Val, resp.Err
	case <-ctx.Done():
		// 注意：请求可能已在另一路径被结算（结果在缓冲通道中），
		// 不重复结算；下次接收者或 GC 会取得该结论，结论不丢失。
		if ctx.Err() == context.Canceled {
			return nil, apiErr(ReasonCanceled, "submit canceled while awaiting result")
		}
		return nil, apiErr(ReasonQueueTimeout, "submit context deadline exceeded while awaiting result")
	}
}

// SubmitChan 异步提交，返回容量为 1 的结果通道，结论恰好写入一次。
func (s *Scheduler) SubmitChan(ctx context.Context, req Request) (<-chan Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return s.submit(ctx, req)
}

func (s *Scheduler) submit(ctx context.Context, req Request) (<-chan Response, error) {
	if req.Model == "" {
		return nil, apiErr(ReasonInvalidConfig, "empty model name")
	}
	ent, ok := s.reg.get(req.Model)
	if !ok {
		return nil, apiErr(ReasonModelNotFound, "model %q is not registered", req.Model)
	}

	s.stateMu.Lock()
	if s.closed {
		s.stateMu.Unlock()
		return nil, ErrSchedulerClosed
	}
	s.stateMu.Unlock()

	e := newEnqueued(ctx, &req, ent.cfg.RequestTimeout, s.now())
	if err := ent.queue.put(ctx, e, ent.cfg.OverflowPolicy); err != nil {
		if ae, ok := err.(*APIError); ok {
			s.metrics.recordReject(req.Model, ae.Reason)
		}
		return nil, err
	}
	s.metrics.recordSubmit(req.Model)
	s.wake()
	return e.done, nil
}

// UpdateConfig 原子热更新完整配置。校验失败、版本不更新或容量收缩
// 不被接受时，返回错误且当前版本完整保留；成功后后续组批全部使用新版本。
func (s *Scheduler) UpdateConfig(cfg Config) error {
	return s.reg.apply(&cfg)
}

// CurrentVersion 返回当前生效配置版本。
func (s *Scheduler) CurrentVersion() int64 { return s.reg.currentVersion() }

// Stats 返回观测快照（队列深度、各类计数、批大小分布与延迟百分位）。
func (s *Scheduler) Stats() SchedulerStats {
	s.stateMu.Lock()
	inFlight := s.inFlight
	s.stateMu.Unlock()
	vw := s.reg.view()
	return s.metrics.snapshot(vw.version, s.now().Sub(s.startTime), inFlight, s.reg.depths())
}

// Close 停止调度：不再接受新批次，在途批次执行完毕并结算，
// 然后关闭所有模型队列（结算仍在排队的请求）。可重复调用。
func (s *Scheduler) Close() {
	s.stateMu.Lock()
	if s.closed {
		s.stateMu.Unlock()
		return
	}
	s.closed = true
	s.stateMu.Unlock()

	close(s.stopCh)
	s.loopWg.Wait() // 调度循环停止，不再投递新批次
	close(s.batches)
	s.workers.stop() // 等待所有在途批次完成并结算
	s.reg.closeAll(ReasonQueueClosed)
}

// wake 通知调度循环立即检查一次（深度触发与回队重试需要）。
func (s *Scheduler) wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *Scheduler) now() time.Time { return time.Now() }

func (s *Scheduler) runLoop() {
	defer s.loopWg.Done()

	// 实际扫描分辨率 = min(TickInterval, windowGranularity)，
	// TickInterval 热更新后在下一拍重置节拍器生效。
	interval := func() time.Duration {
		d := s.reg.view().global.TickInterval
		if d <= 0 || d > windowGranularity {
			return windowGranularity
		}
		return d
	}
	tick := time.NewTicker(interval())
	defer tick.Stop()
	lastTick := interval()

	for {
		select {
		case <-s.stopCh:
			return
		case <-s.wakeCh:
		case <-tick.C:
			if d := interval(); d != lastTick {
				tick.Reset(d)
				lastTick = d
			}
		}
		if s.stopped() {
			return
		}
		s.reg.notifyWaiters()
		s.formAndDispatch()
	}
}

func (s *Scheduler) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// formAndDispatch 执行一个调度节拍的组批（规则见 batch.go 注释）。
//
// 两阶段处理，避免“每拍取出又放回”的抖动：
//  1. 只窥视各队队首，判定本拍是否应当出发（任一模型深度触发或窗口触发）；
//  2. 只有确实要出发时才 drainUpTo，此时对取出的超时/取消条目立即结算，
//     其余条目构成子批。未被任何触发命中的模型本轮完全不动。
func (s *Scheduler) formAndDispatch() {
	v := s.reg.view()
	now := s.now()

	names := make([]string, 0, len(v.models))
	for name := range v.models {
		names = append(names, name)
	}
	sortStrings(names)

	type modelDecision struct {
		name string
		ent  *modelEntry
		send bool
	}
	// 阶段 0：清理每个模型队首连续的超时/取消请求，使其立即得到可区分
	// 结论（无需等待批次出发），并腾出队列槽位唤醒阻塞提交者。
	for _, name := range names {
		ent := v.models[name]
		stale := ent.queue.drainStale(now)
		for _, e := range stale {
			reason := staleReason(e, now)
			if reason == ReasonNone {
				reason = ReasonCanceled
			}
			if e.settle(nil, staleError(reason, name)) {
				s.metrics.recordSettle(name, reason)
			}
		}
	}

	decisions := make([]modelDecision, 0, len(names))
	triggered := false
	for _, name := range names {
		ent := v.models[name]
		cfg := ent.cfg
		oldest, full, _ := ent.queue.headInfo(cfg.MaxBatchSize, now)
		if oldest.IsZero() {
			continue
		}
		windowHit := now.Sub(oldest) >= cfg.BatchWindow
		send := full || windowHit
		if send {
			triggered = true
		}
		decisions = append(decisions, modelDecision{name: name, ent: ent, send: send})
	}
	if !triggered {
		return
	}

	groups := make([]batchGroup, 0, len(decisions))
	for _, d := range decisions {
		if !d.send {
			continue
		}
		cfg := d.ent.cfg
		drained := d.ent.queue.drainUpTo(cfg.MaxBatchSize)
		items := make([]batchItem, 0, len(drained))
		for _, e := range drained {
			if reason := staleReason(e, now); reason != ReasonNone {
				if e.settle(nil, staleError(reason, d.name)) {
					s.metrics.recordSettle(d.name, reason)
				}
				continue
			}
			items = append(items, batchItem{entry: e, attempt: e.attempts + 1})
		}
		if len(items) == 0 {
			continue
		}
		groups = append(groups, batchGroup{
			model:      d.name,
			items:      items,
			queue:      d.ent.queue,
			strategy:   cfg.RetryStrategy,
			maxRetries: cfg.MaxRetries,
		})
	}
	if len(groups) == 0 {
		return
	}

	b := &batch{
		id:      atomic.AddInt64(&s.nextID, 1),
		version: v.version,
		groups:  groups,
		created: now,
	}

	s.stateMu.Lock()
	s.inFlight += totalItems(groups)
	s.stateMu.Unlock()

	// 非阻塞投递：所有工作协程都忙（批次通道满）时，把本批各子批
	// 原样放回队首，保持 FIFO，调度循环继续服务其他模型并稍后重试。
	// 这保证一个慢模型的拥塞不会卡住全局调度。
	select {
	case s.batches <- b:
	default:
		// 回退后 inFlight 归零；每个模型的在途条目放回其组批来源队列。
		// 该队列若已因模型移除而关闭，则确定性结算为 queue_closed，
		// 名额同样归还，系统内总占用不泄漏。
		for gi := range groups {
			g := &groups[gi]
			entries := make([]*enqueued, len(g.items))
			for j, it := range g.items {
				entries[j] = it.entry
			}
			if g.queue.closedFlag() {
				g.queue.settleInFlight(entries, ErrQueueClosed)
			} else {
				g.queue.requeueHead(entries)
			}
		}
		s.stateMu.Lock()
		s.finishBatchLocked(b)
		s.stateMu.Unlock()
		s.wake()
	}
}

// finishBatch 由工作协程在整批结算完成后回调。
func (s *Scheduler) finishBatch(b *batch) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.inFlight -= b.totalItems()
	if s.inFlight < 0 {
		s.inFlight = 0
	}
}

func (s *Scheduler) finishBatchLocked(b *batch) {
	s.inFlight -= b.totalItems()
	if s.inFlight < 0 {
		s.inFlight = 0
	}
}

func totalItems(groups []batchGroup) int {
	n := 0
	for _, g := range groups {
		n += len(g.items)
	}
	return n
}

// staleReason 判断条目在出发前是否已超时/取消。超时优先于取消，
// 使两类结论对调用方可区分。
func staleReason(e *enqueued, now time.Time) Reason {
	if e.expired(now) {
		return ReasonQueueTimeout
	}
	if e.ctx != nil && e.ctx.Err() != nil {
		if e.ctx.Err() == context.Canceled {
			return ReasonCanceled
		}
		return ReasonQueueTimeout
	}
	return ReasonNone
}

func staleError(reason Reason, model string) error {
	switch reason {
	case ReasonQueueTimeout:
		return apiErr(ReasonQueueTimeout, "request timed out while queued for model %q", model)
	case ReasonCanceled:
		return apiErr(ReasonCanceled, "request canceled while queued")
	default:
		return nil
	}
}

func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}
