package serving

import (
	"sort"
	"sync"
	"time"
)

// maxLatencySamples 限制批延迟保留的样本数，使内存占用有界。
const maxLatencySamples = 1024

// ModelStats 是单个模型的观测快照（值类型，拷贝后即可安全读取）。
type ModelStats struct {
	Model       string
	QueueDepth  int
	InFlight    int // 已出队、尚未最终结论（含回队重试前）的条目数
	Outstanding int // QueueDepth + InFlight，系统内该模型的总占用
	QueueCap    int

	Submitted   int64 // 成功进入队列的请求数
	Completed   int64 // 成功返回结果的请求数
	Rejected    int64 // 队列满/队列关闭等入队前拒绝数
	TimedOut    int64 // 等待超时结算数（含队列槽等待与组批等待）
	Canceled    int64 // 被调用方取消的结算数
	Failed      int64 // 不可重试失败 / 未配置重试的失败数
	RetryExh    int64 // 重试耗尽的失败数
	Retried     int64 // 触发过的重试次数（attempt 增加）
	BatchesSent int64 // 该模型被发出的子批数

	// BatchSizeDist: 子批大小 -> 出现次数。
	BatchSizeDist map[int]int64
}

// SchedulerStats 是全局观测快照。
type SchedulerStats struct {
	Version            int64
	UptimeMillis       int64
	InFlight           int   // 已出发批次中尚未结算的条目数
	BatchesSent        int64 // 已发出的混合批次数
	BatchItems         int64 // 已发出批次覆盖的条目数
	BatchLatencyMicros []int64
	// BatchLatencyPercentiles: p50/p90/p99（微秒），-1 表示暂无样本。
	BatchLatencyPercentiles map[string]int64
	Models                  []ModelStats
}

type modelMetrics struct {
	submitted   int64
	completed   int64
	rejected    int64
	timedOut    int64
	canceled    int64
	failed      int64
	retryExh    int64
	retried     int64
	batchesSent int64
	sizeDist    map[int]int64
}

// metrics 聚合各模型计数与批次直方图。
type metrics struct {
	mu       sync.Mutex
	perModel map[string]*modelMetrics

	batchesSent int64
	batchItems  int64
	latencies   []int64 // 有界环形采样（微秒）
	latPos      int
	latFilled   bool
}

func newMetrics() *metrics {
	return &metrics{
		perModel:  make(map[string]*modelMetrics),
		latencies: make([]int64, maxLatencySamples),
	}
}

func (m *metrics) mm(model string) *modelMetrics {
	mm, ok := m.perModel[model]
	if !ok {
		mm = &modelMetrics{sizeDist: make(map[int]int64)}
		m.perModel[model] = mm
	}
	return mm
}

func (m *metrics) recordSubmit(model string) {
	m.mu.Lock()
	m.mm(model).submitted++
	m.mu.Unlock()
}

func (m *metrics) recordReject(model string, reason Reason) {
	m.mu.Lock()
	mm := m.mm(model)
	mm.rejected++
	switch reason {
	case ReasonQueueTimeout:
		mm.timedOut++
	case ReasonCanceled:
		mm.canceled++
	}
	m.mu.Unlock()
}

// recordSettle 记录一个请求的最终结论（成功或各类失败）。
func (m *metrics) recordSettle(model string, reason Reason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mm := m.mm(model)
	switch reason {
	case ReasonNone:
		mm.completed++
	case ReasonQueueTimeout:
		mm.timedOut++
	case ReasonCanceled:
		mm.canceled++
	case ReasonRetryExhausted:
		mm.retryExh++
		mm.failed++
	default:
		mm.failed++
	}
}

func (m *metrics) recordRetry(model string) {
	m.mu.Lock()
	m.mm(model).retried++
	m.mu.Unlock()
}

func (m *metrics) recordBatch(sizes map[string]int, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchesSent++
	for model, n := range sizes {
		mm := m.mm(model)
		mm.batchesSent++
		mm.sizeDist[n]++
		m.batchItems += int64(n)
	}
	m.latencies[m.latPos] = latency.Microseconds()
	m.latPos++
	if m.latPos == len(m.latencies) {
		m.latPos = 0
		m.latFilled = true
	}
}

// snapshot 生成快照；depth 由调用方按模型名提供当前队列深度。
func (m *metrics) snapshot(version int64, uptime time.Duration, inFlight int,
	depths map[string]queueDepthCap) SchedulerStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.perModel))
	for k := range m.perModel {
		names = append(names, k)
	}
	sort.Strings(names)

	stats := SchedulerStats{
		Version:                 version,
		UptimeMillis:            uptime.Milliseconds(),
		InFlight:                inFlight,
		BatchesSent:             m.batchesSent,
		BatchItems:              m.batchItems,
		BatchLatencyPercentiles: map[string]int64{"p50": -1, "p90": -1, "p99": -1},
		Models:                  make([]ModelStats, 0, len(names)),
	}

	n := len(m.latencies)
	if !m.latFilled {
		n = m.latPos
	}
	if n > 0 {
		samples := make([]int64, n)
		if m.latFilled {
			copy(samples, m.latencies[m.latPos:])
			copy(samples[len(m.latencies)-m.latPos:], m.latencies[:m.latPos])
		} else {
			copy(samples, m.latencies[:n])
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		stats.BatchLatencyMicros = samples
		stats.BatchLatencyPercentiles["p50"] = samples[(n*50)/100:][:1][0]
		stats.BatchLatencyPercentiles["p90"] = samples[min((n*90)/100, n-1)]
		stats.BatchLatencyPercentiles["p99"] = samples[min((n*99)/100, n-1)]
	}

	for _, name := range names {
		mm := m.perModel[name]
		dist := make(map[int]int64, len(mm.sizeDist))
		for k, v := range mm.sizeDist {
			dist[k] = v
		}
		ms := ModelStats{
			Model:         name,
			Submitted:     mm.submitted,
			Completed:     mm.completed,
			Rejected:      mm.rejected,
			TimedOut:      mm.timedOut,
			Canceled:      mm.canceled,
			Failed:        mm.failed,
			RetryExh:      mm.retryExh,
			Retried:       mm.retried,
			BatchesSent:   mm.batchesSent,
			BatchSizeDist: dist,
		}
		if dc, ok := depths[name]; ok {
			ms.QueueDepth = dc.depth
			ms.InFlight = dc.inflight
			ms.Outstanding = dc.depth + dc.inflight
			ms.QueueCap = dc.cap
		}
		stats.Models = append(stats.Models, ms)
	}
	return stats
}

type queueDepthCap struct {
	depth    int
	inflight int
	cap      int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
