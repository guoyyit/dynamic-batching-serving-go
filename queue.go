package serving

import (
	"context"
	"sync"
	"time"
)

// modelQueue 是单个模型的有界 FIFO 请求队列。
//
// 背压语义：
//   - OverflowReject：队列满时 put 立即返回 ErrQueueFull；
//   - OverflowBlock：put 阻塞等待空位。等待期间不额外创建定时器/协程，
//     由调度循环每次出队后 Broadcast 唤醒，因此取消/超时的感知延迟
//     不超过一个调度节拍。调用方自身的 goroutine 是阻塞的唯一成本，
//     且在队请求数量永远不超过 QueueCapacity。
type modelQueue struct {
	model    string
	capacity int

	mu      sync.Mutex
	items   []*enqueued
	out     int // 已出队但尚未得到最终结论（或尚未回队）的在途条目数
	notFull *sync.Cond
	closed  bool
	waiters int // 正在阻塞等待空位的提交者数量
}

// 在途名额（out）记账规则：
// 一个条目 drainUpTo 出队后计入 out，直到它被最终结算（release）或
// 重新回到队列（requeueHead）。因此任意时刻 len(items)+out <= capacity，
// “批次通道满回退”与“回队重试”都不会让系统内条目超过队列上界。

// checkInvariantLocked 在持锁状态下断言容量不变量（开发期保护）。
func (q *modelQueue) checkInvariantLocked(tag string) {
	if len(q.items)+q.out > q.capacity {
		panic("serving: capacity invariant violated at " + tag)
	}
}

func newModelQueue(model string, capacity int) *modelQueue {
	q := &modelQueue{
		model:    model,
		capacity: capacity,
		items:    make([]*enqueued, 0, capacity),
	}
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// Len 返回当前队列深度。
func (q *modelQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Cap 返回队列容量。
func (q *modelQueue) Cap() int { return q.capacity }

// isClosed 调用方持锁时使用。
func (q *modelQueue) isClosed() bool { return q.closed }

// put 按 policy 把条目放入队尾。
func (q *modelQueue) put(ctx context.Context, e *enqueued, policy OverflowPolicy) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if policy == OverflowBlock {
		q.waiters++
		for len(q.items)+q.out >= q.capacity && !q.closed {
			// 在等待前先看取消预检查（取消/超时通常由调度循环每拍唤醒后感知）。
			if err := waitError(ctx, e, time.Now()); err != nil {
				q.waiters--
				return err
			}
			q.notFull.Wait()
		}
		q.waiters--
		if q.closed {
			return ErrQueueClosed
		}
		if err := waitError(ctx, e, time.Now()); err != nil {
			return err
		}
		q.items = append(q.items, e)
		q.checkInvariantLocked("put-block")
		return nil
	}

	// reject（默认）
	if q.closed {
		return ErrQueueClosed
	}
	if len(q.items)+q.out >= q.capacity {
		return ErrQueueFull
	}
	q.items = append(q.items, e)
	q.checkInvariantLocked("put-reject")
	return nil
}

// waitError 判定阻塞等待期间应返回的可区分结论：
// 排队超时优先于调用方取消（超时是本系统的一等语义）。
func waitError(ctx context.Context, e *enqueued, now time.Time) error {
	if e.expired(now) {
		return apiErr(ReasonQueueTimeout, "request timed out while waiting for model %q queue slot", e.req.Model)
	}
	if ctx != nil && ctx.Err() != nil {
		if ctx.Err() == context.Canceled {
			return apiErr(ReasonCanceled, "request canceled while waiting for queue slot")
		}
		return apiErr(ReasonQueueTimeout, "request context deadline exceeded while waiting for queue slot")
	}
	return nil
}

// headInfo 在不移除条目的情况下返回队首等待时长相关信息：
// oldest 为队首条目入队时间（空队列返回零值），full 表示当前深度
// 是否达到 maxBatchSize，stale 表示队首已超时/取消。
func (q *modelQueue) headInfo(maxBatchSize int, now time.Time) (oldest time.Time, full, stale bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return time.Time{}, false, false
	}
	head := q.items[0]
	stale = head.expired(now) || (head.ctx != nil && head.ctx.Err() != nil)
	return head.enqAt, len(q.items) >= maxBatchSize, stale
}

// drainStale 取出队首连续的已超时/取消条目（不触发批次的清理）。
func (q *modelQueue) drainStale(now time.Time) []*enqueued {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for n < len(q.items) {
		e := q.items[n]
		if !e.expired(now) && (e.ctx == nil || e.ctx.Err() == nil) {
			break
		}
		n++
	}
	if n == 0 {
		return nil
	}
	out := make([]*enqueued, n)
	copy(out, q.items[:n])
	q.items = q.items[n:]
	q.notFull.Broadcast()
	return out
}

// drainUpTo 取出队首最多 n 个条目（保持 FIFO），同时不得超过剩余在途
// 名额（capacity - out），保证“在队 + 在途”总数永远不超过容量。
// 取出即脱离队列并计入 out，空位立即对阻塞提交者可见，但总占用不变。
func (q *modelQueue) drainUpTo(n int) []*enqueued {
	q.mu.Lock()
	defer q.mu.Unlock()
	limit := q.capacity - q.out
	if n > limit {
		n = limit
	}
	if n <= 0 || len(q.items) == 0 {
		return nil
	}
	if n > len(q.items) {
		n = len(q.items)
	}
	out := make([]*enqueued, n)
	copy(out, q.items[:n])
	q.items = q.items[n:]
	q.out += n
	q.checkInvariantLocked("drain")
	if cap(q.items) > q.capacity*2 {
		// 防止长时间运行后底层数组只缩容逻辑长度不缩容物理长度。
		ns := make([]*enqueued, len(q.items), q.capacity)
		copy(ns, q.items)
		q.items = ns
	}
	q.notFull.Broadcast()
	return out
}

// release 表示 n 个在途条目已得到最终结论，归还其占用的名额。
func (q *modelQueue) release(n int) {
	if n <= 0 {
		return
	}
	q.mu.Lock()
	if q.out >= n {
		q.out -= n
	} else if q.out > 0 {
		q.out = 0
	}
	q.checkInvariantLocked("release")
	q.notFull.Broadcast()
	q.mu.Unlock()
}

// snapshot 返回同一把锁内一致的 深度/在途/容量。
func (q *modelQueue) snapshot() (depth, inflight, capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items), q.out, q.capacity
}

// outstanding 返回在途条目数（供观测/测试断言有界性）。
func (q *modelQueue) outstanding() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.out
}

// peekSnapshot 返回队列内容的浅拷贝（用于配置变更时迁移）。
func (q *modelQueue) peekSnapshot() []*enqueued {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*enqueued, len(q.items))
	copy(out, q.items)
	return out
}

// clear 取走全部在队条目并广播空位。
func (q *modelQueue) clear() []*enqueued {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.items
	q.items = make([]*enqueued, 0, q.capacity)
	q.notFull.Broadcast()
	return out
}

// requeueHead 将重试项（来自在途集合）按给定顺序放回一个“打开中”的
// 队列队首。调用方必须确保该队列未关闭。items[0] 在最前；这些条目
// 释放在途名额、重新占用队列槽位，总占用保持不变。
func (q *modelQueue) requeueHead(items []*enqueued) {
	if len(items) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.out >= len(items) {
		q.out -= len(items)
	}
	merged := make([]*enqueued, 0, len(items)+len(q.items))
	merged = append(merged, items...)
	merged = append(merged, q.items...)
	q.items = merged
	q.checkInvariantLocked("requeueHead")
}

// settleInFlight 关闭路径使用：把在途条目以关闭原因结算，并归还其名额。
func (q *modelQueue) settleInFlight(items []*enqueued, err error) {
	if len(items) == 0 {
		return
	}
	for _, e := range items {
		e.settle(nil, err)
	}
	q.release(len(items))
}

// closedFlag 返回队列是否已关闭（热更新迁移或模型移除）。
func (q *modelQueue) closedFlag() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// notifyWaiters 唤醒阻塞中的提交者，使其有机会重新检查取消/超时。
// 仅在确有等待者时才广播。
func (q *modelQueue) notifyWaiters() {
	q.mu.Lock()
	if q.waiters > 0 {
		q.notFull.Broadcast()
	}
	q.mu.Unlock()
}

// close 关闭队列：后续 put 立即失败，并把在队请求全部以 reason 结算。
func (q *modelQueue) close(reason Reason) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	err := apiErr(reason, "model %q queue closed", q.model)
	for _, e := range q.items {
		e.settle(nil, err)
	}
	q.items = nil
	q.notFull.Broadcast()
}
