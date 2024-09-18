package snowflake

import (
	"errors"
	"sync"
	"time"
)

// 定义常量
const (
	epoch          = 1704038400000 // 2024-01-01的时间戳
	workerIDBits   = 10            // 机器ID所占的位数
	sequenceBits   = 12            // 序列号所占的位数
	maxWorkerID    = -1 ^ (-1 << workerIDBits)
	maxSequence    = -1 ^ (-1 << sequenceBits)
	workerIDShift  = sequenceBits
	timestampShift = workerIDBits + sequenceBits
)

// 定义结构体
type Snowflake struct {
	mu            sync.Mutex
	workerID      int64
	lastTimestamp int64
	sequence      int64
}

// 创建Snowflake实例
func NewSnowflake(workerID int64) (*Snowflake, error) {
	if workerID < 0 || workerID > maxWorkerID {
		return nil, errors.New("invalid worker ID")
	}
	return &Snowflake{
		workerID:      workerID,
		lastTimestamp: 0,
		sequence:      0,
	}, nil
}

// 生成ID的方法
func (s *Snowflake) NextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	timestamp := time.Now().UnixMilli()
	if timestamp < s.lastTimestamp {
		panic("clock moved backwards")
	}

	if timestamp == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			// 当前毫秒的序列号已经用完，等待下一毫秒
			for timestamp <= s.lastTimestamp {
				timestamp = time.Now().UnixMilli()
			}
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = timestamp

	id := ((timestamp - epoch) << timestampShift) | (s.workerID << workerIDShift) | s.sequence
	return id
}
