package shardkv

import (
	"sort"
	"sync"
)

type SimpleOrderedMap struct {
	mu   sync.RWMutex
	data map[string]string
	keys []string
	less func(a, b string) bool
}

func NewSimpleOrderedMap(less func(a, b string) bool) SimpleOrderedMap {
	return SimpleOrderedMap{
		data: make(map[string]string),
		less: less,
	}
}

func (m *SimpleOrderedMap) Set(key string, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[key]; !exists {
		m.keys = append(m.keys, key)
		sort.Slice(m.keys, func(i, j int) bool { return m.less(m.keys[i], m.keys[j]) })
	}
	m.data[key] = value
}

func (m *SimpleOrderedMap) Append(key string, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[key]; !exists {
		m.keys = append(m.keys, key)
		sort.Slice(m.keys, func(i, j int) bool { return m.less(m.keys[i], m.keys[j]) })
	}
	m.data[key] += value
}

func (m *SimpleOrderedMap) Get(key string) (value string, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok = m.data[key]
	return
}

func (m *SimpleOrderedMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		delete(m.data, key)
		// 从 keys 中删除
		idx := -1
		for i, k := range m.keys {
			if k == key {
				idx = i
				break
			}
		}
		if idx >= 0 {
			m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
		}
	}
}

func (m *SimpleOrderedMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func (m *SimpleOrderedMap) Ascend(f func(key string, value string) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, k := range m.keys {
		if !f(k, m.data[k]) {
			break
		}
	}
}

// After 返回从指定 key 之后（大于 key）的 n 个键值对。
// 如果 key 不存在，则从大于 key 的第一个键开始取。
// 返回的实际元素个数可能少于 n（如果剩余不足）。
// 如果 n <= 0，返回空map。
func (m *SimpleOrderedMap) After(key string, n int) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string)

	if n <= 0 || len(m.keys) == 0 {
		return result
	}

	// 二分查找第一个大于等于 key 的索引
	idx := sort.Search(len(m.keys), func(i int) bool {
		return !m.less(m.keys[i], key) // 等价于 m.keys[i] >= key
	})

	// 如果找到的 key 等于给定 key，跳过它（取之后）
	if idx < len(m.keys) && m.keys[idx] == key {
		idx++
	}

	if idx >= len(m.keys) {
		return result
	}

	// 计算实际可取的个数
	end := idx + n
	if end > len(m.keys) {
		end = len(m.keys)
	}

	for i := idx; i < end; i++ {
		k := m.keys[i]
		result[k] = m.data[k]
	}
	return result
}
