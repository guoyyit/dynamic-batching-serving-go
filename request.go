package serving

import (
	"context"
	"sync"
	"time"
)

// Request 是一次本地推理请求。Payload 对调度器不透明，原样传给 Inferencer。
type Request struct {
	Model   string
	Payload any
}

// Response 是返还给原调用方的最终结论（结果或错误，二者恰有一个非零）。
type Response struct {
	Val any
	Err error
}

// enqueued 是队列中的一个条目，携带请求自己的响应通道。
// done 通道容量为 1，结算方只需发送一次；settle 用 sync.Once 保证
// 无论超时/取消/执行路径如何竞争，每个请求恰好得到一次结论。
type enqueued struct {
	req      *Request
	ctx      context.Context
	deadline time.Time // 零值表示无截止时间
	enqAt    time.Time

	done       chan Response
	settleOnce sync.Once

	// attempts 记录已发生/已开始的 Infer 尝试次数（含跨 requeue 批次），
	// 保证重试总次数受 MaxRetries 约束。
	attempts int
}

func newEnqueued(ctx context.Context, req *Request, timeout time.Duration, now time.Time) *enqueued {
	var dl time.Time
	if timeout > 0 {
		dl = now.Add(timeout)
	}
	return &enqueued{
		req:      req,
		ctx:      ctx,
		deadline: dl,
		enqAt:    now,
		done:     make(chan Response, 1),
	}
}

// expired 判断在 now 时刻是否已超过排队/等待截止时间。
func (e *enqueued) expired(now time.Time) bool {
	return !e.deadline.IsZero() && !now.Before(e.deadline)
}

// settle 恰好一次地把结论送入响应通道。返回 true 表示本次调用完成了结算。
func (e *enqueued) settle(val any, err error) bool {
	did := false
	e.settleOnce.Do(func() {
		did = true
		e.done <- Response{Val: val, Err: err}
	})
	return did
}
