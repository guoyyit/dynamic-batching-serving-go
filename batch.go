package serving

import "time"

// batchItem 是批次中的单个成员，attempt 从 1 开始计数该请求的执行尝试。
type batchItem struct {
	entry   *enqueued
	attempt int
}

// batchGroup 是批次内发往同一模型的子批。其成员严格来自该模型的 FIFO
// 队列、保持入队顺序，且数量不超过该模型当前版本配置的 MaxBatchSize。
type batchGroup struct {
	model string
	items []batchItem
	// queue 是组批时取出这些条目的队列实例。工作协程把在途名额
	// 归还/转回给这个实例，而不是按模型名重新查找——热更新可能已经
	// 用新容量队列替换了实例，绑定引用保证记账不会错放到旧/新队列。
	queue *modelQueue
	// 组批时刻该模型的版本参数，工作协程按这份参数执行与重试，
	// 保证“批次与其参数一致”，不被执行期间的热更新改变。
	strategy   RetryStrategy
	maxRetries int
}

// batch 是一个已出发的混合批次。
//
// 确定性的组批（合并）规则（见 docs/batching.md）：
//  1. 每个调度节拍扫描所有模型；模型名按字典序处理，保证输出确定；
//  2. 每个模型按 FIFO 从队首取出至多 MaxBatchSize 个有效条目
//     （已超时/已取消的条目在取出时直接结算、不进入批次）；
//  3. 各模型子批按模型名字典序排列在 groups 中；
//  4. 一个批次只在以下任一条件成立时出发：至少一个模型达到
//     MaxBatchSize（深度触发），或至少一个模型的最早请求等待时间
//     达到其 BatchWindow（窗口触发）；
//  5. 批次携带出发时的配置版本号，其所有子批都与该版本一致。
//
// 拆分规则：工作协程按 group 逐个调用 Inferencer；每个 group 独立结算，
// 一个模型失败不影响批次内其他模型；单项结果按下标返还，不跨模型混淆。
type batch struct {
	id      int64
	version int64
	groups  []batchGroup
	created time.Time
}

// totalItems 返回批次总条目数。
func (b *batch) totalItems() int {
	n := 0
	for _, g := range b.groups {
		n += len(g.items)
	}
	return n
}

// sizes 返回各模型组大小（用于观测）。
func (b *batch) sizes() map[string]int {
	m := make(map[string]int, len(b.groups))
	for _, g := range b.groups {
		m[g.model] = len(g.items)
	}
	return m
}
