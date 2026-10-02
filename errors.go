package sched

import "errors"

// 调度器可区分的错误结论。
var (
	// ErrQueueFull 队列达到上界且背压策略为拒绝。
	ErrQueueFull = errors.New("sched: queue is full")
	// ErrTimeout 请求在排队或执行期间超过了调用方给定的截止时间。
	ErrTimeout = errors.New("sched: request timed out")
	// ErrCancelled 请求被调用方主动取消。
	ErrCancelled = errors.New("sched: request cancelled")
	// ErrModelNotFound 请求的模型未注册。
	ErrModelNotFound = errors.New("sched: model not found")
	// ErrModelRemoved 请求的模型已被移除。
	ErrModelRemoved = errors.New("sched: model removed")
	// ErrEngineFailed 推理执行失败（重试耗尽或不可重试）。
	ErrEngineFailed = errors.New("sched: inference failed")
	// ErrShutdown 调度器已关闭，请求未被处理。
	ErrShutdown = errors.New("sched: scheduler is shut down")
)
