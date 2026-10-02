package serving

import "fmt"

// Reason 以机器可读的方式区分一个请求最终结论的原因。
// 调用方可以通过 *APIError 的 Reason 字段（或 errors.As）区分
// 超时、取消、队列满、模型不存在、执行失败等不同结论。
type Reason string

const (
	ReasonNone            Reason = ""
	ReasonModelNotFound   Reason = "model_not_found"
	ReasonQueueFull       Reason = "queue_full"
	ReasonQueueClosed     Reason = "queue_closed"
	ReasonQueueTimeout    Reason = "queue_timeout"
	ReasonCanceled        Reason = "canceled"
	ReasonFailed          Reason = "failed"
	ReasonRetryExhausted  Reason = "retry_exhausted"
	ReasonSchedulerClosed Reason = "scheduler_closed"
	ReasonInvalidConfig   Reason = "invalid_config"
)

// APIError 是本包对外返回的带原因错误，调用方可按 Reason 区分处理。
type APIError struct {
	Reason Reason
	Msg    string
	Err    error
}

func (e *APIError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Reason, e.Msg, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Msg)
}

func (e *APIError) Unwrap() error { return e.Err }

func apiErr(reason Reason, msg string, args ...any) *APIError {
	return &APIError{Reason: reason, Msg: fmt.Sprintf(msg, args...)}
}

func wrapErr(reason Reason, msg string, cause error) *APIError {
	return &APIError{Reason: reason, Msg: msg, Err: cause}
}

// 哨兵错误，便于 errors.Is 判断。
var (
	ErrModelNotFound   = &APIError{Reason: ReasonModelNotFound, Msg: "model not found"}
	ErrQueueFull       = &APIError{Reason: ReasonQueueFull, Msg: "model queue is full"}
	ErrQueueClosed     = &APIError{Reason: ReasonQueueClosed, Msg: "model queue is closed"}
	ErrSchedulerClosed = &APIError{Reason: ReasonSchedulerClosed, Msg: "scheduler is closed"}
	ErrInvalidConfig   = &APIError{Reason: ReasonInvalidConfig, Msg: "invalid configuration"}
)
