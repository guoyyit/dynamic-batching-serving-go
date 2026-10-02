package serving

import "time"

// OverflowPolicy 决定模型队列达到上界时 Submit 的行为。
type OverflowPolicy string

const (
	// OverflowReject 立即以 ReasonQueueFull 拒绝（背压给调用方）。
	OverflowReject OverflowPolicy = "reject"
	// OverflowBlock 阻塞等待空位，等待期间仍受 ctx 取消/请求超时约束。
	OverflowBlock OverflowPolicy = "block"
)

// RetryStrategy 决定批内单项失败且错误可重试时的处理方式。
// 无论哪种策略，成功的单项不会被重复执行。
type RetryStrategy string

const (
	// RetryNever 不重试，单项以失败结算。
	RetryNever RetryStrategy = "never"
	// RetryImmediate 在同一批次的执行循环内立即重试，最多 MaxRetries 次；
	// 超过后按 RetryExhausted 结算。
	RetryImmediate RetryStrategy = "immediate"
	// RetryRequeue 把失败单项放回所属模型队列的队首，等待后续批次；
	// 仍受请求超时约束，且总尝试次数上限为 MaxRetries+1。
	RetryRequeue RetryStrategy = "requeue"
)

// 默认值，配置中零值字段在 Validate/normalize 时被填充，保证语义确定。
const (
	defaultTickInterval   = 2 * time.Millisecond
	defaultNumWorkers     = 2
	defaultMaxBatchSize   = 32
	defaultBatchWindow    = 5 * time.Millisecond
	defaultQueueCapacity  = 128
	defaultRequestTimeout = 30 * time.Second
	defaultMaxRetries     = 0
)

// ModelConfig 是单个模型的批处理参数。同一次组批看到的 ModelConfig
// 永远来自同一个配置版本（见 registry 的快照机制）。
type ModelConfig struct {
	Name string

	// MaxBatchSize 发往该模型的单个子批最大条目数。
	MaxBatchSize int
	// BatchWindow 该模型首个请求入队后，最多等待多久就必须触发组批。
	BatchWindow time.Duration
	// QueueCapacity 该模型等待队列的上界（含在队请求）。
	QueueCapacity int
	// RequestTimeout 请求从入队到被批次消费的最大等待时间，超时以
	// ReasonQueueTimeout 结算；0 表示使用默认值。
	RequestTimeout time.Duration

	// MaxRetries 可重试失败的最大额外尝试次数（0 = 不重试）。
	MaxRetries int
	// RetryStrategy 重试策略：never / immediate / requeue。
	RetryStrategy RetryStrategy
	// OverflowPolicy 队列满策略：reject / block。
	OverflowPolicy OverflowPolicy
}

// GlobalConfig 是调度器全局参数。
type GlobalConfig struct {
	// TickInterval 调度循环基础节拍，决定组批的时间分辨率。
	TickInterval time.Duration
	// NumWorkers 消费批次的工作协程数，固定大小，不随负载增长。
	NumWorkers int
}

// Config 是一份完整配置快照；Models 以模型名为键。
type Config struct {
	// Version 必须为正数；热更新只接受更大的版本，避免旧配置覆盖新配置。
	Version int64
	Global  GlobalConfig
	Models  map[string]ModelConfig
}

// normalize 填充默认值。
func (c *Config) normalize() {
	if c.Global.TickInterval <= 0 {
		c.Global.TickInterval = defaultTickInterval
	}
	if c.Global.NumWorkers <= 0 {
		c.Global.NumWorkers = defaultNumWorkers
	}
	for name, mc := range c.Models {
		if mc.Name == "" {
			mc.Name = name
		}
		if mc.MaxBatchSize <= 0 {
			mc.MaxBatchSize = defaultMaxBatchSize
		}
		if mc.BatchWindow <= 0 {
			mc.BatchWindow = defaultBatchWindow
		}
		if mc.QueueCapacity <= 0 {
			mc.QueueCapacity = defaultQueueCapacity
		}
		if mc.RequestTimeout <= 0 {
			mc.RequestTimeout = defaultRequestTimeout
		}
		if mc.RetryStrategy == "" {
			mc.RetryStrategy = RetryNever
		}
		if mc.OverflowPolicy == "" {
			mc.OverflowPolicy = OverflowReject
		}
		if mc.MaxRetries < 0 {
			mc.MaxRetries = 0
		}
		c.Models[name] = mc
	}
}

// Validate 校验配置；非法时返回带 ReasonInvalidConfig 的错误，调用方
// 据此可知热更新被拒绝、旧配置仍生效。
func (c *Config) Validate() error {
	if c == nil {
		return wrapErr(ReasonInvalidConfig, "config is nil", nil)
	}
	if c.Version <= 0 {
		return apiErr(ReasonInvalidConfig, "config version must be positive, got %d", c.Version)
	}
	if c.Global.TickInterval < 0 {
		return apiErr(ReasonInvalidConfig, "tick interval must not be negative")
	}
	if c.Global.NumWorkers < 0 {
		return apiErr(ReasonInvalidConfig, "num workers must not be negative")
	}
	if c.Models == nil {
		return apiErr(ReasonInvalidConfig, "models map is nil")
	}
	for name, mc := range c.Models {
		if mc.Name != "" && mc.Name != name {
			return apiErr(ReasonInvalidConfig, "model %q has mismatched Name %q", name, mc.Name)
		}
		if mc.MaxBatchSize < 0 || mc.BatchWindow < 0 || mc.QueueCapacity < 0 ||
			mc.RequestTimeout < 0 || mc.MaxRetries < 0 {
			return apiErr(ReasonInvalidConfig, "model %q has negative field", name)
		}
		switch mc.RetryStrategy {
		case RetryNever, RetryImmediate, RetryRequeue, "":
		default:
			return apiErr(ReasonInvalidConfig, "model %q has unknown retry strategy %q", name, mc.RetryStrategy)
		}
		switch mc.OverflowPolicy {
		case OverflowReject, OverflowBlock, "":
		default:
			return apiErr(ReasonInvalidConfig, "model %q has unknown overflow policy %q", name, mc.OverflowPolicy)
		}
		if mc.MaxRetries > 0 && mc.RetryStrategy == RetryNever {
			return apiErr(ReasonInvalidConfig, "model %q sets MaxRetries but strategy never", name)
		}
	}
	return nil
}

// clone 返回配置的深拷贝（含 Models），避免调用方在更新后修改影响内部版本。
func (c *Config) clone() *Config {
	if c == nil {
		return nil
	}
	cp := &Config{
		Version: c.Version,
		Global:  c.Global,
		Models:  make(map[string]ModelConfig, len(c.Models)),
	}
	for k, v := range c.Models {
		cp.Models[k] = v
	}
	return cp
}
