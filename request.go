package sched

import (
	"context"
	"sync"
	"time"
)

// Request 表示一个推理请求。
type Request struct {
	// ID 请求唯一标识，用于结果返还与重试去重。
	ID string
	// Model 目标模型名。
	Model string
	// Payload 推理输入（本地桩原样回显）。
	Payload []byte

	ctx         context.Context
	done        chan Outcome
	once        sync.Once
	submittedAt time.Time
}

// Outcome 是返还给调用方的结论：成功输出或可区分的错误。
type Outcome struct {
	ReqID string
	Model string
	// Output 成功时的推理结果。
	Output []byte
	// Err 失败原因，使用 errors.Is 可区分 ErrTimeout/ErrCancelled/ErrQueueFull 等。
	Err error
}

// finish 恰好一次地写入请求结论（通道有缓冲，写入不会阻塞）。
// 返回 true 表示本次调用完成了结论写入。
func (r *Request) finish(o Outcome) bool {
	done := false
	r.once.Do(func() {
		o.ReqID = r.ID
		o.Model = r.Model
		r.done <- o
		done = true
	})
	return done
}

// alive 判断请求在给定时刻是否仍未超时/取消。
func (r *Request) alive() bool {
	return r.ctx.Err() == nil
}
