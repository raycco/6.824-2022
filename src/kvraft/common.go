package kvraft

import "6.824/raft"

const (
	dKvServer raft.LogTopic = "KVSR"
	dKvClient raft.LogTopic = "KVCL"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoAgreement = "ErrNoAgreement"
	ErrOutOfOrder  = "ErrOutOfOrder"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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

type Reply struct {
	ok    bool
	Err   Err
	Value string
}
