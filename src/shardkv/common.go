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
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrWrongMigrate  = "ErrWrongMigrate"
	ErrWaitCfgChange = "ErrWaitCfgChange"
)

const (
	dKvServer raft.LogTopic = "KVSR"
	dKvClient raft.LogTopic = "KVCL"
)

type State int

const (
	ACTIVING  State = 0x01
	CONFIGING State = 0x02
	SERVING   State = 0x04
	WAITING   State = 0x08
	MIGRATING State = 0x10
	DELETING  State = 0x20
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
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int64
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
