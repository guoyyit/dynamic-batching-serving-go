package sched

import (
	"errors"
	"sync"
)

// LatencyStats 延迟统计（毫秒）。
type LatencyStats struct {
	Count   int64
	Min     float64
	Max     float64
	Avg     float64
	Buckets map[string]int64
}

// ModelMetrics 单个模型的可观测指标。
type ModelMetrics struct {
	QueueDepth int
	Submitted  int64
	Completed  int64
	Failed     int64
	Timeout    int64
	Cancelled  int64
	QueueFull  int64
	Retried    int64
	// BatchSizes 该模型分组的大小分布：size -> count。
	BatchSizes map[int]int64
	Latency    LatencyStats
}

// Totals 全局指标。
type Totals struct {
	Submitted int64
	Completed int64
	Failed    int64
	Timeout   int64
	Cancelled int64
	QueueFull int64
	Retried   int64
	Batches   int64
	// BatchSizes 整批总大小分布：size -> count。
	BatchSizes map[int]int64
	Latency    LatencyStats
}

// MetricsSnapshot 是某一时刻的指标快照。
type MetricsSnapshot struct {
	Models map[string]ModelMetrics
	Totals Totals
}

// metricsRegistry 内部指标注册表。
type metricsRegistry struct {
	mu     sync.Mutex
	models map[string]*modelMetrics
	totals totals
}

type modelMetrics struct {
	submitted, completed, failed, timeout, cancelled, queueFull, retried int64
	batchSizes                                                           map[int]int64
	latency                                                              *latencyHistogram
}

type totals struct {
	submitted, completed, failed, timeout, cancelled, queueFull, retried, batches int64
	batchSizes                                                                    map[int]int64
	latency                                                                       *latencyHistogram
}

type latencyHistogram struct {
	count   int64
	sum     float64
	min     float64
	max     float64
	buckets map[string]int64
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{
		min:     -1,
		buckets: map[string]int64{},
	}
}

// latencyBuckets 定义延迟桶（毫秒），确定且可文档化。
var latencyBuckets = []struct {
	label string
	upper float64
}{
	{"1ms", 1}, {"2ms", 2}, {"5ms", 5}, {"10ms", 10},
	{"25ms", 25}, {"50ms", 50}, {"100ms", 100}, {"250ms", 250},
	{"500ms", 500}, {"1s", 1000}, {"2.5s", 2500}, {"5s", 5000},
}

func (h *latencyHistogram) record(ms float64) {
	h.count++
	h.sum += ms
	if h.min < 0 || ms < h.min {
		h.min = ms
	}
	if ms > h.max {
		h.max = ms
	}
	for _, b := range latencyBuckets {
		if ms <= b.upper {
			h.buckets[b.label]++
			return
		}
	}
	h.buckets["+Inf"]++
}

func (h *latencyHistogram) snapshot() LatencyStats {
	st := LatencyStats{
		Count:   h.count,
		Min:     0,
		Max:     h.max,
		Buckets: map[string]int64{},
	}
	if h.count > 0 {
		st.Min = h.min
		st.Avg = h.sum / float64(h.count)
	}
	for _, b := range latencyBuckets {
		st.Buckets[b.label] = h.buckets[b.label]
	}
	st.Buckets["+Inf"] = h.buckets["+Inf"]
	return st
}

func newMetricsRegistry() *metricsRegistry {
	return &metricsRegistry{
		models: map[string]*modelMetrics{},
		totals: totals{
			batchSizes: map[int]int64{},
			latency:    newLatencyHistogram(),
		},
	}
}

func (m *metricsRegistry) model(name string) *modelMetrics {
	mm, ok := m.models[name]
	if !ok {
		mm = &modelMetrics{
			batchSizes: map[int]int64{},
			latency:    newLatencyHistogram(),
		}
		m.models[name] = mm
	}
	return mm
}

// recordSubmitted 记录一次成功入队。
func (m *metricsRegistry) recordSubmitted(model string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.model(model).submitted++
	m.totals.submitted++
}

// recordQueueFull 记录一次队列满拒绝。
func (m *metricsRegistry) recordQueueFull(model string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.model(model).queueFull++
	m.totals.queueFull++
}

// recordOutcome 按结论类型记录完成/失败/超时/取消与延迟。
func (m *metricsRegistry) recordOutcome(model string, o Outcome, latencyMS float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mm := m.model(model)
	mm.latency.record(latencyMS)
	m.totals.latency.record(latencyMS)
	switch {
	case o.Err == nil:
		mm.completed++
		m.totals.completed++
	case errors.Is(o.Err, ErrTimeout):
		mm.timeout++
		m.totals.timeout++
	case errors.Is(o.Err, ErrCancelled):
		mm.cancelled++
		m.totals.cancelled++
	default:
		mm.failed++
		m.totals.failed++
	}
}

// recordRetry 记录一次重试。
func (m *metricsRegistry) recordRetry(model string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.model(model).retried++
	m.totals.retried++
}

// recordBatch 记录一个批次：总大小分布与各模型分组大小分布。
func (m *metricsRegistry) recordBatch(totalSize int, groupSizes map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totals.batches++
	m.totals.batchSizes[totalSize]++
	for name, size := range groupSizes {
		mm := m.model(name)
		mm.batchSizes[size]++
	}
}

// snapshot 导出快照；queueDepthOf 用于读取各模型实时队列深度。
func (m *metricsRegistry) snapshot(queueDepthOf func(model string) int) MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap := MetricsSnapshot{
		Models: map[string]ModelMetrics{},
		Totals: Totals{
			Submitted:  m.totals.submitted,
			Completed:  m.totals.completed,
			Failed:     m.totals.failed,
			Timeout:    m.totals.timeout,
			Cancelled:  m.totals.cancelled,
			QueueFull:  m.totals.queueFull,
			Retried:    m.totals.retried,
			Batches:    m.totals.batches,
			BatchSizes: copyIntMap(m.totals.batchSizes),
			Latency:    m.totals.latency.snapshot(),
		},
	}
	for name, mm := range m.models {
		snap.Models[name] = ModelMetrics{
			QueueDepth: queueDepthOf(name),
			Submitted:  mm.submitted,
			Completed:  mm.completed,
			Failed:     mm.failed,
			Timeout:    mm.timeout,
			Cancelled:  mm.cancelled,
			QueueFull:  mm.queueFull,
			Retried:    mm.retried,
			BatchSizes: copyIntMap(mm.batchSizes),
			Latency:    mm.latency.snapshot(),
		}
	}
	return snap
}

func copyIntMap(in map[int]int64) map[int]int64 {
	out := make(map[int]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
