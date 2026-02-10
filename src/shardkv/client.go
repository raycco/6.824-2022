package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"6.824/snowflake"
)

var GlobalClientId int64 = 0

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64
	currentSeqId int64
	snowflake    *snowflake.Snowflake
	grpLeaderId  map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = GlobalClientId
	GlobalClientId++
	var err error
	ck.snowflake, err = snowflake.NewSnowflake(ck.clientId)
	if err != nil {
		panic(err)
	}
	ck.currentSeqId = ck.snowflake.NextID()
	ck.grpLeaderId = make(map[int]int)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	args.ClientId = ck.clientId
	ck.currentSeqId = ck.snowflake.NextID()
	args.SeqId = ck.currentSeqId

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Num = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			srv := ck.make_end(servers[ck.grpLeaderId[gid]])
			var reply GetReply
			ok := srv.Call("ShardKV.Get", &args, &reply)
			raft.LogPrint(raft.INFO, dKvClient, "C%d G%d recv Get response from S%d args %+v reply %+v",
				ck.clientId, gid, ck.grpLeaderId[gid], args, reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}

			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				if si == ck.grpLeaderId[gid] {
					continue
				}
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				raft.LogPrint(raft.INFO, dKvClient, "C%d G%d recv Get response from S%d args %+v reply %+v",
					ck.clientId, gid, si, args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.grpLeaderId[gid] = si
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	args.ClientId = ck.clientId
	ck.currentSeqId = ck.snowflake.NextID()
	args.SeqId = ck.currentSeqId

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Num = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			srv := ck.make_end(servers[ck.grpLeaderId[gid]])
			var reply PutAppendReply
			ok := srv.Call("ShardKV.PutAppend", &args, &reply)
			raft.LogPrint(raft.INFO, dKvClient, "C%d G%d recv Put/Append response from S%d args %+v reply %+v",
				ck.clientId, gid, ck.grpLeaderId[gid], args, reply)
			if ok && reply.Err == OK {
				return
			}

			for si := 0; si < len(servers); si++ {
				if si == ck.grpLeaderId[gid] {
					continue
				}
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				raft.LogPrint(raft.INFO, dKvClient, "C%d G%d recv Put/Append response from S%d args %+v reply %+v",
					ck.clientId, gid, si, args, reply)
				if ok && reply.Err == OK {
					ck.grpLeaderId[gid] = si
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
