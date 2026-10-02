package sched

import "sort"

// Batch 是一次发往工作协程的组批单元。
// 同一批次可以包含多个模型的请求，但每个模型分组的大小不超过该模型的 MaxBatchSize。
// 批次一旦组批完成并派发，其内容不可变（只读）。
type Batch struct {
	// ID 批次递增编号。
	ID int64
	// groups 按模型名分组的请求（组批期间由 batcher 单协程写入，派发后只读）。
	groups map[string][]*Request
	// size 批次总请求数。
	size int
	// cfg 组批时锁定的配置版本（组批期间配置更新不影响本批）。
	cfg *configVersion
}

// newBatch 创建一个空批次。
func newBatch(cfg *configVersion, id int64) *Batch {
	return &Batch{
		ID:     id,
		groups: map[string][]*Request{},
		cfg:    cfg,
	}
}

// add 按模型把请求加入批次，返回加入后该模型分组的大小。
func (b *Batch) add(r *Request) int {
	b.groups[r.Model] = append(b.groups[r.Model], r)
	b.size++
	return len(b.groups[r.Model])
}

// modelSize 返回某模型分组当前大小。
func (b *Batch) modelSize(model string) int {
	return len(b.groups[model])
}

// models 返回排序后的模型名列表（确定顺序，纯计算无副作用）。
func (b *Batch) models() []string {
	return sortedModels(b.groups)
}

// group 返回某模型分组。
func (b *Batch) group(model string) []*Request {
	return b.groups[model]
}

// sortedModels 对模型名做确定排序。
func sortedModels(m map[string][]*Request) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
