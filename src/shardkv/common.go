package shardkv

import "6.824/raft"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	Empty              = ""
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrConfigChange    = "ErrConfigChange"
	ErrConfigExpired   = "ErrConfigExpired"
	ErrMigrateComplete = "ErrMigrateComplete"
	ErrMigrating       = "ErrMigrating"
)

const (
	dKvServer raft.LogTopic = "KVSR"
	dKvClient raft.LogTopic = "KVCL"
)

const (
	ACTIVING  = 0x01
	CONFIGING = 0x02
	SERVING   = 0x04
	WAITING   = 0x08
	PUSHING   = 0x10
	PULLING   = 0x20
	MIGRATING = 0x40
	DELETING  = 0x80
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int64
	Num      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int64
	Num      int
}

type GetReply struct {
	Err   Err
	Value string
}

func containsKey(slice []string, key string) bool {
	for _, v := range slice {
		if v == key {
			return true
		}
	}
	return false
}

func removeKey(slice []string, key string) []string {
	for i, v := range slice {
		if v == key {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func removeShard(slice []int, shard int) []int {
	for i := 0; i < len(slice); {
		if slice[i] == shard {
			slice = append(slice[:i], slice[i+1:]...)
		} else {
			i++
		}
	}
	return slice
}

func containsShard(slice []int, shard int) bool {
	for _, v := range slice {
		if v == shard {
			return true
		}
	}
	return false
}
