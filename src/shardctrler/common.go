package shardctrler

import (
	"sort"

	"6.824/raft"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	Empty          = ""
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	dScServer raft.LogTopic = "SCSR"
	dScClient raft.LogTopic = "SCCL"
)

type CommonArgs struct {
	ClientId int64
	SeqId    int64
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ClientId int64
	SeqId    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClientId int64
	SeqId    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientId int64
	SeqId    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClientId int64
	SeqId    int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Reply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type GidShards struct {
	gid    int
	shards []int
}

func createGidShardsArr(gidShardsMap map[int][]int) []GidShards {
	gidShardsArr := make([]GidShards, 0)
	for gid, shardids := range gidShardsMap {
		sort.Ints(shardids)
		gidShardsArr = append(gidShardsArr, GidShards{gid, shardids})
	}
	return gidShardsArr
}

func sortGidShardsArray(gidshards []GidShards) {
	sort.SliceStable(gidshards, func(i, j int) bool {
		if len(gidshards[i].shards) == len(gidshards[j].shards) {
			return gidshards[i].gid < gidshards[j].gid
		}
		return len(gidshards[i].shards) < len(gidshards[j].shards)
	})
}
