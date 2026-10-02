package serving

import (
	"context"
	"sync"
	"time"
)

// workerPool 是固定数量的批处理工作协程池：协程数由 GlobalConfig.NumWorkers
// 决定，负载升高时不新建协程，从根本上限制内存与协程的增长上界。
type workerPool struct {
	n       int
	engine  Inferencer
	batches <-chan *batch

	wg      sync.WaitGroup
	metrics *metrics
	sched   *Scheduler
}

func newWorkerPool(n int, engine Inferencer, batches <-chan *batch, mt *metrics, s *Scheduler) *workerPool {
	return &workerPool{n: n, engine: engine, batches: batches, metrics: mt, sched: s}
}

func (p *workerPool) start() {
	for i := 0; i < p.n; i++ {
		p.wg.Add(1)
		go p.loop()
	}
}

func (p *workerPool) loop() {
	defer p.wg.Done()
	for b := range p.batches {
		p.executeBatch(context.Background(), b)
	}
}

func (p *workerPool) stop() { p.wg.Wait() }

// executeBatch 执行一个混合批次。
//
// 确定性执行/失败规则（见 docs/batching.md）：
//   - groups 按批次内顺序（模型名字典序）逐个调用 Inferencer，
//     一个模型组的失败绝不影响其他组的结果；
//   - 单项失败只结算该单项；整批错误只重发尚未成功的成员，
//     成功成员不会被再次送入 Infer（不产生重复副作用）；
//   - immediate：在本次执行内对失败成员重试至多 maxRetries 次；
//   - requeue：把失败成员放回所属模型队首（保持失败顺序），由后续
//     批次继续尝试，总尝试次数仍受 maxRetries 约束；
//   - 不可重试错误立即失败；可重试错误超过上限以 retry_exhausted 结算；
//   - 执行期间请求被取消/超时：尚未执行的在途成员在每次尝试前被检查，
//     立即以可区分原因结算且不调用 Infer；已在 Infer 中的成员等其返回。
func (p *workerPool) executeBatch(ctx context.Context, b *batch) {
	for gi := range b.groups {
		g := &b.groups[gi]
		p.executeGroup(ctx, g)
	}
	latency := time.Since(b.created)
	p.metrics.recordBatch(b.sizes(), latency)
	p.sched.finishBatch(b)
}

func (p *workerPool) executeGroup(ctx context.Context, g *batchGroup) {
	model := g.model
	total := len(g.items)
	returned := 0
	// pending 是仍需一次 Infer 的成员（保持顺序）。
	pending := make([]batchItem, 0, len(g.items))
	pending = append(pending, g.items...)

	for len(pending) > 0 {
		// 尝试前过滤在途期间已超时/取消的成员；被过滤者不再调用 Infer。
		ready, skipped := p.filterReady(pending)
		for _, it := range skipped {
			p.settleCanceledOrTimeout(it.entry)
		}
		pending = ready
		if len(pending) == 0 {
			break
		}

		payloads := make([]any, len(pending))
		for i, it := range pending {
			it.entry.attempts++
			it.attempt = it.entry.attempts
			pending[i] = it
			payloads[i] = it.entry.req.Payload
		}

		results, err := p.engine.Infer(ctx, model, payloads)
		if err != nil {
			// 整批错误：对全部待处理成员应用失败/重试判定。
			ie, _ := err.(*InferError)
			retryable := ie != nil && ie.Retryable
			next := make([]batchItem, 0, len(pending))
			for _, it := range pending {
				if retryable && it.entry.attempts <= g.maxRetries {
					p.metrics.recordRetry(model)
					next = append(next, it)
					continue
				}
				p.failItem(model, it, err, retryable, it.entry.attempts > 1)
			}
			var requeued int
			pending, requeued = p.handleRetry(model, g, next)
			returned += requeued
			continue
		}

		// 部分失败/逐项结算。结果必须与输入等长对齐；不等长视为整批失败。
		if len(results) != len(pending) {
			alignErr := &InferError{Retryable: false, Msg: "inferencer returned misaligned results"}
			for _, it := range pending {
				p.failItem(model, it, alignErr, false, false)
			}
			pending = nil
			continue
		}
		next := make([]batchItem, 0)
		for i, it := range pending {
			r := results[i]
			if r.Err == nil {
				it.entry.settle(r.Output, nil)
				p.metrics.recordSettle(model, ReasonNone)
				continue
			}
			if r.Err.Retryable && it.entry.attempts <= g.maxRetries {
				p.metrics.recordRetry(model)
				next = append(next, it)
				continue
			}
			p.failItem(model, it, r.Err, r.Err.Retryable, it.entry.attempts > 1)
		}
		var requeued int
		pending, requeued = p.handleRetry(model, g, next)
		returned += requeued
	}

	// 最终归还的在途名额 = 总成员 - 已真正回队（重新占用队列槽位）的成员。
	g.queue.release(total - returned)
}

// handleRetry 返回下一轮仍需在本批次内立即重试的成员，以及真正被放回
// 队列（requeue 策略）的成员数量。
func (p *workerPool) handleRetry(model string, g *batchGroup, next []batchItem) ([]batchItem, int) {
	if len(next) == 0 {
		return nil, 0
	}
	if g.strategy != RetryRequeue {
		return next, 0 // immediate：仍在途，由本循环继续重试
	}
	entries := make([]*enqueued, len(next))
	for i, it := range next {
		entries[i] = it.entry
	}
	// 回队重试优先放回当前打开的队列（可能是热更新后的新容量队列，
	// 在途名额已在迁移时转入）；模型已被移除时，把在途条目确定性地
	// 结算为关闭并归还名额，绝不静默丢弃。
	if ent, ok := p.sched.reg.get(model); ok && !ent.queue.closedFlag() {
		ent.queue.requeueHead(entries)
	} else {
		for _, e := range entries {
			if e.settle(nil, ErrQueueClosed) {
				p.metrics.recordSettle(model, ReasonQueueClosed)
			}
		}
		g.queue.release(len(entries))
	}
	p.sched.wake()
	return nil, len(next)
}

// filterReady 把在途期间已取消/超时的成员拆出，保证它们不会被送入 Infer。
func (p *workerPool) filterReady(items []batchItem) (ready, skipped []batchItem) {
	now := p.sched.now()
	for _, it := range items {
		e := it.entry
		if e.expired(now) {
			skipped = append(skipped, it)
			continue
		}
		if e.ctx != nil && e.ctx.Err() != nil {
			skipped = append(skipped, it)
			continue
		}
		ready = append(ready, it)
	}
	return ready, skipped
}

func (p *workerPool) settleCanceledOrTimeout(e *enqueued) {
	now := p.sched.now()
	if e.expired(now) {
		if e.settle(nil, apiErr(ReasonQueueTimeout, "request timed out before execution for model %q", e.req.Model)) {
			p.metrics.recordSettle(e.req.Model, ReasonQueueTimeout)
		}
		return
	}
	if e.ctx != nil && e.ctx.Err() == context.Canceled {
		if e.settle(nil, apiErr(ReasonCanceled, "request canceled before execution")) {
			p.metrics.recordSettle(e.req.Model, ReasonCanceled)
		}
		return
	}
	if e.ctx != nil && e.ctx.Err() != nil {
		if e.settle(nil, apiErr(ReasonQueueTimeout, "request context deadline exceeded before execution")) {
			p.metrics.recordSettle(e.req.Model, ReasonQueueTimeout)
		}
		return
	}
}

// failItem 以确定性原因结算失败成员。
// exhausted 表示该成员此前确实发生过重试（attempts>1），此时使用
// ReasonRetryExhausted；否则按 ReasonFailed 结算。
func (p *workerPool) failItem(model string, it batchItem, cause error, retryable, exhausted bool) {
	e := it.entry
	var err error
	if retryable && exhausted {
		err = wrapErr(ReasonRetryExhausted, "retry budget exhausted", cause)
		if e.settle(nil, err) {
			p.metrics.recordSettle(model, ReasonRetryExhausted)
		}
		return
	}
	if retryable {
		err = wrapErr(ReasonFailed, "retryable failure not retried by policy", cause)
	} else {
		err = wrapErr(ReasonFailed, "inference failed", cause)
	}
	if e.settle(nil, err) {
		p.metrics.recordSettle(model, ReasonFailed)
	}
}
