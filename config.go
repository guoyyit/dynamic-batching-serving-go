package sched

import (
	"fmt"
	"time"
)

// BackpressurePolicy 队列达到上界时的背压策略。
type BackpressurePolicy int

const (
	// BackpressureReject 队列满时立即拒绝，返回 ErrQueueFull。
	BackpressureReject BackpressurePolicy = iota
	// BackpressureBlock 队列满时阻塞等待，直到入队成功或请求超时/取消。
	BackpressureBlock
)

// ModelConfig 单个模型的批处理参数与队列容量。
type ModelConfig struct {
	// MaxBatchSize 发往该模型的一个批次中允许的最大请求数（>0）。
	MaxBatchSize int
	// MaxQueueSize 该模型等待队列的容量（>0），与其他模型队列隔离。
	MaxQueueSize int
	// MaxRetries 可重试失败的最大重试次数（>=0）。
	MaxRetries int
	// RetryBackoff 两次重试之间的退避时长（>=0）。
	RetryBackoff time.Duration
	// Backpressure 队列达到上界时的策略。
	Backpressure BackpressurePolicy
}

// Config 调度器的完整配置。配置以版本化方式原子生效。
type Config struct {
	// BatchWindow 批窗口：批次从开始组批起的最长等待时间。
	BatchWindow time.Duration
	// Workers 并发消费批次的工作协程数（>0）。
	Workers int
	// MaxBatchSize 单批总请求数上限（所有模型合计），0 表示不限。
	MaxBatchSize int
	// Models 各模型的独立参数。
	Models map[string]ModelConfig
}

// configVersion 是 Config 的一个不可变版本快照。
type configVersion struct {
	version int64
	cfg     Config
}

// validate 校验配置合法性，非法配置不会替换当前可用配置。
func (c Config) validate() error {
	if c.BatchWindow <= 0 {
		return fmt.Errorf("sched: BatchWindow must be > 0, got %v", c.BatchWindow)
	}
	if c.Workers <= 0 {
		return fmt.Errorf("sched: Workers must be > 0, got %d", c.Workers)
	}
	if c.MaxBatchSize < 0 {
		return fmt.Errorf("sched: MaxBatchSize must be >= 0, got %d", c.MaxBatchSize)
	}
	if len(c.Models) == 0 {
		return fmt.Errorf("sched: at least one model must be configured")
	}
	for name, m := range c.Models {
		if name == "" {
			return fmt.Errorf("sched: model name must not be empty")
		}
		if m.MaxBatchSize <= 0 {
			return fmt.Errorf("sched: model %q MaxBatchSize must be > 0, got %d", name, m.MaxBatchSize)
		}
		if m.MaxQueueSize <= 0 {
			return fmt.Errorf("sched: model %q MaxQueueSize must be > 0, got %d", name, m.MaxQueueSize)
		}
		if m.MaxRetries < 0 {
			return fmt.Errorf("sched: model %q MaxRetries must be >= 0, got %d", name, m.MaxRetries)
		}
		if m.RetryBackoff < 0 {
			return fmt.Errorf("sched: model %q RetryBackoff must be >= 0, got %v", name, m.RetryBackoff)
		}
		if m.Backpressure != BackpressureReject && m.Backpressure != BackpressureBlock {
			return fmt.Errorf("sched: model %q invalid Backpressure policy %d", name, m.Backpressure)
		}
	}
	return nil
}
