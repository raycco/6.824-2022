package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/snowflake"
)

var GlobalClientId int64 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId     int64
	leaderId     int
	currentSeqId int64
	snowflake    *snowflake.Snowflake
	dead         int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = GlobalClientId
	GlobalClientId++
	ck.leaderId = 0
	var err error
	ck.snowflake, err = snowflake.NewSnowflake(ck.clientId)
	if err != nil {
		panic(err)
	}
	ck.currentSeqId = ck.snowflake.NextID()
	return ck
}

func (ck *Clerk) GetClientId() int64 {
	return ck.clientId
}

func (kv *Clerk) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *Clerk) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	result := ck.Request(OP_QUERY_STR, args)
	reply := result.(*QueryReply)
	return reply.Config
	/*for {
		var reply QueryReply

		if ck.killed() {
			return reply.Config
		}

		raft.LogPrint(raft.INFO, dScClient, "C%d send Query request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok1 := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Query response from S%d ok %v reply %+v", ck.clientId, ck.leaderId, ok1, reply)
		if ok1 && reply.Err == OK {
			return reply.Config
		}
		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply QueryReply
			ok2 := srv.Call("ShardCtrler.Query", args, &reply)
			if ok2 && reply.Err == OK {
				ck.leaderId = svrid
				return reply.Config
			}
		}

		time.Sleep(100 * time.Millisecond)
	}*/
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	ck.Request(OP_JOIN_STR, args)

	/*for {
		var reply JoinReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Join request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok1 := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Join response from S%d ok %v reply %+v", ck.clientId, ck.leaderId, ok1, reply)
		if ok1 && reply.Err == OK {
			return
		}
		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply JoinReply
			ok2 := srv.Call("ShardCtrler.Join", args, &reply)
			if ok2 && reply.Err == OK {
				ck.leaderId = svrid
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}*/
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	ck.Request(OP_LEAVE_STR, args)
	/*for {
		var reply LeaveReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Leave request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok1 := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Leave response from S%d ok %v reply %+v", ck.clientId, ck.leaderId, ok1, reply)
		if ok1 && reply.Err == OK {
			return
		}

		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply LeaveReply
			ok2 := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok2 && reply.Err == OK {
				ck.leaderId = svrid
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}*/
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	ck.Request(OP_MOVE_STR, args)
	/*for {
		var reply MoveReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Move request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok1 := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Move response from S%d ok %v reply %+v", ck.clientId, ck.leaderId, ok1, reply)
		if ok1 && reply.Err == OK {
			return
		}
		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply MoveReply
			ok2 := srv.Call("ShardCtrler.Move", args, &reply)
			if ok2 && reply.Err == OK {
				ck.leaderId = svrid
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}*/
}

func (ck *Clerk) Request(opstr string, args interface{}) interface{} {

	method := OP_PREFIX + opstr

	var reply CommonReply

	count := 0
	srvid := ck.leaderId
	for {

		var ok bool

		// try each known server.
		srv := ck.servers[srvid]
		raft.LogPrint(raft.INFO, dScClient, "C%d send %s request to S%d args %+v", ck.clientId, opstr, srvid, args)

		switch opstr {
		case OP_QUERY_STR:
			concreteReply := &QueryReply{}
			ok = srv.Call(method, args, concreteReply)
			reply = concreteReply
		case OP_JOIN_STR:
			concreteReply := &JoinReply{}
			ok = srv.Call(method, args, concreteReply)
			reply = concreteReply
		case OP_LEAVE_STR:
			concreteReply := &LeaveReply{}
			ok = srv.Call(method, args, concreteReply)
			reply = concreteReply
		case OP_MOVE_STR:
			concreteReply := &MoveReply{}
			ok = srv.Call(method, args, concreteReply)
			reply = concreteReply
		}

		raft.LogPrint(raft.INFO, dScClient, "C%d receive %s response from S%d %v reply %+v", ck.clientId, opstr, srvid, ok, reply)

		if ok && reply.GetErr() == OK {
			ck.leaderId = srvid
			return reply
		} else {
			srvid = (srvid + 1) % len(ck.servers)
		}

		count++
		if count%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}

		if ck.killed() {
			return reply
		}
	}
}
