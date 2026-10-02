package serving

import "sync"

// modelEntry 是调度器内某模型的运行时状态（配置 + 有界队列）。
type modelEntry struct {
	cfg   ModelConfig
	queue *modelQueue
}

// registry 持有当前配置版本与各模型运行时状态。
//
// 并发协议（保证“组批与更新落到某个完整版本”）：
//   - 每个调度节拍在 RLock 下读取版本与全部 entry，组批完成后释放；
//   - UpdateConfig 在 Lock 内完成全部队列的新建/迁移/关闭，
//     因此一次组批看到的要么全是旧版本、要么全是新版本，不会混合；
//   - 容量变更通过“新建队列 + 迁移在队条目”原子完成；旧队列被关闭，
//     仍持有旧队列引用的在途批次照常完成，其结果通道不受影响。
type registry struct {
	mu      sync.RWMutex
	version int64
	global  GlobalConfig
	models  map[string]*modelEntry
}

func newRegistry(cfg *Config) *registry {
	r := &registry{
		version: cfg.Version,
		global:  cfg.Global,
		models:  make(map[string]*modelEntry, len(cfg.Models)),
	}
	for name, mc := range cfg.Models {
		r.models[name] = &modelEntry{
			cfg:   mc,
			queue: newModelQueue(name, mc.QueueCapacity),
		}
	}
	return r
}

// view 是一个配置版本下的一致性只读快照。
type registryView struct {
	version int64
	global  GlobalConfig
	models  map[string]*modelEntry
}

// view 获取当前完整版本快照（持读锁期间调用方不得阻塞）。
func (r *registry) view() registryView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return registryView{version: r.version, global: r.global, models: r.models}
}

func (r *registry) get(model string) (*modelEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.models[model]
	return e, ok
}

func (r *registry) currentVersion() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}

// apply 原子应用一份完整配置。
// 规则：cfg 必须通过 Validate；Version 必须大于当前版本。
// 任一步骤失败则返回错误且上一个可用配置完整保留。
//   - 新增模型：创建空队列；
//   - 已有模型：更新配置，仅当容量变化时迁移队列；
//   - 移除模型：关闭其队列，在队请求以 queue_closed 结算。
func (r *registry) apply(cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	cfg = cfg.clone()
	cfg.normalize()

	r.mu.Lock()
	defer r.mu.Unlock()

	if cfg.Version <= r.version {
		return apiErr(ReasonInvalidConfig,
			"config version %d is not newer than current version %d", cfg.Version, r.version)
	}

	// 先做纯内存检查：若旧队列中现存条目超过新容量，拒绝本次更新，
	// 旧配置完整保留（队列尚未被改动）。
	for name, mc := range cfg.Models {
		if old, ok := r.models[name]; ok && mc.QueueCapacity < old.queue.Len() {
			return apiErr(ReasonInvalidConfig,
				"cannot shrink model %q queue to %d while %d requests are queued",
				name, mc.QueueCapacity, old.queue.Len())
		}
	}

	next := make(map[string]*modelEntry, len(cfg.Models))
	for name, mc := range cfg.Models {
		if old, ok := r.models[name]; ok {
			e := &modelEntry{cfg: mc}
			if mc.QueueCapacity == old.queue.Cap() {
				e.queue = old.queue // 容量不变：复用，FIFO 连续
			} else {
				e.queue = migrateQueue(old.queue, name, mc.QueueCapacity)
			}
			next[name] = e
			continue
		}
		next[name] = &modelEntry{cfg: mc, queue: newModelQueue(name, mc.QueueCapacity)}
	}
	// 被移除的模型：结算其在队请求。
	for name, old := range r.models {
		if _, keep := next[name]; !keep {
			old.queue.close(ReasonQueueClosed)
		}
	}

	r.models = next
	r.global = cfg.Global
	r.version = cfg.Version
	return nil
}

// migrateQueue 把旧队列当前全部在队条目与在途名额迁移到新容量队列，
// 保持 FIFO。在途名额（out）一并转移，保证已出发但可能回队的条目
// 仍被新容量计数，迁移后 len(items)+out <= capacity 恒成立。
func migrateQueue(old *modelQueue, model string, capacity int) *modelQueue {
	items := old.clear()
	nq := newModelQueue(model, capacity)
	old.mu.Lock()
	nq.out = old.out
	old.out = 0
	old.mu.Unlock()
	if len(items)+nq.out > capacity {
		// apply 已提前拦截容量小于在队长度；含在途仍溢出时，
		// 多余的在队条目确定性地以 queue_closed 结算。
		room := capacity - nq.out
		if room < 0 {
			room = 0
		}
		if len(items) > room {
			for _, e := range items[room:] {
				e.settle(nil, apiErr(ReasonQueueClosed, "model %q queue capacity changed", model))
			}
			items = items[:room]
		}
	}
	nq.mu.Lock()
	nq.items = append(nq.items, items...)
	nq.mu.Unlock()
	old.retire()
	return nq
}

// retire 仅标记关闭并唤醒阻塞提交者，不结算条目（条目已被迁移接管）。
func (q *modelQueue) retire() {
	q.mu.Lock()
	q.closed = true
	q.notFull.Broadcast()
	q.mu.Unlock()
}

// closeAll 关闭全部模型队列（调度器关闭时使用）。
func (r *registry) closeAll(reason Reason) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.models {
		e.queue.close(reason)
	}
}

// notifyWaiters 唤醒所有模型队列上的阻塞提交者（每个调度节拍调用，
// 使 block 等待者的取消/超时在一个节拍内被感知，且不新建任何协程）。
func (r *registry) notifyWaiters() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.models {
		e.queue.notifyWaiters()
	}
}

// depths 收集队列深度/容量用于观测快照。
func (r *registry) depths() map[string]queueDepthCap {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]queueDepthCap, len(r.models))
	for name, e := range r.models {
		d, inf, c := e.queue.snapshot()
		out[name] = queueDepthCap{depth: d, inflight: inf, cap: c}
	}
	return out
}
