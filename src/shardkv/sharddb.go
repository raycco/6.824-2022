package shardkv

import (
	"sort"
)

const (
	KEY_MIN = "-"
	KEY_MAX = "="
)

type ShardDb struct {
	KvData  map[string]string
	Keys    []string
	LastKey string
}

func NewShardDb() *ShardDb {
	return &ShardDb{
		KvData:  make(map[string]string),
		Keys:    make([]string, 0),
		LastKey: KEY_MAX,
	}
}

func (db *ShardDb) Set(key string, value string) {
	if _, exists := db.KvData[key]; !exists {
		db.Keys = append(db.Keys, key)
		sort.Slice(db.Keys, func(i, j int) bool { return db.Keys[i] < db.Keys[j] })
	}
	db.KvData[key] = value
}

func (db *ShardDb) Append(key string, value string) {
	if _, exists := db.KvData[key]; !exists {
		db.Keys = append(db.Keys, key)
		sort.Slice(db.Keys, func(i, j int) bool { return db.Keys[i] < db.Keys[j] })
	}
	db.KvData[key] += value
}

func (db *ShardDb) Get(key string) (value string, ok bool) {
	value, ok = db.KvData[key]
	return
}

func (db *ShardDb) Delete(key string) {
	if _, ok := db.KvData[key]; ok {
		delete(db.KvData, key)
		// 从 Keys 中删除
		idx := -1
		for i, k := range db.Keys {
			if k == key {
				idx = i
				break
			}
		}
		if idx >= 0 {
			db.Keys = append(db.Keys[:idx], db.Keys[idx+1:]...)
		}
	}
}

func (db *ShardDb) Len() int {
	return len(db.KvData)
}

func (db *ShardDb) Ascend(f func(key string, value string) bool) {
	for _, k := range db.Keys {
		if !f(k, db.KvData[k]) {
			break
		}
	}
}

func (db *ShardDb) IsLastKeyMax() bool {
	return db.LastKey == KEY_MAX
}

func (db *ShardDb) IsLastKeyMin() bool {
	return db.LastKey == KEY_MIN
}

func (db *ShardDb) SetLastKey(key string) {
	db.LastKey = key
}

func (db *ShardDb) GetLastKey() string {
	return db.LastKey
}

// From 返回从指定 key 开始（包括该 key）的 n 个键值对。
// 如果 key 不存在，则从大于 key 的第一个键开始取。
func (db *ShardDb) From(key string, n int) (map[string]string, string) {

	result := make(map[string]string)

	if n <= 0 || len(db.Keys) == 0 {
		return result, KEY_MAX
	}

	if key == KEY_MIN {
		key = db.Keys[0]
	}

	// 二分查找第一个大于等于 key 的索引
	idx := sort.Search(len(db.Keys), func(i int) bool {
		return db.Keys[i] >= key
	})

	// 如果找到的 key 等于给定 key，跳过它（取之后）
	/*if idx < len(db.Keys) && db.Keys[idx] == key {
		idx++
	}*/

	if idx >= len(db.Keys) {
		return result, KEY_MAX
	}

	// 计算实际可取的个数
	end := idx + n
	if end > len(db.Keys) {
		end = len(db.Keys)
	}

	for i := idx; i < end; i++ {
		k := db.Keys[i]
		result[k] = db.KvData[k]
	}
	if end == len(db.Keys) {
		return result, KEY_MAX
	} else {
		return result, db.Keys[end-1]
	}
}
