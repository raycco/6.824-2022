package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"reflect"
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId
	for {
		var reply QueryReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Query request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Query response to S%d reply %+v", ck.clientId, ck.leaderId, reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		}
		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = svrid
				return reply.Config
			}
		}
		/*reply := ck.tryEachServer(OP_QUERY, args)
		if !reply.(QueryReply).WrongLeader {
			return reply.(QueryReply).Config
		}*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	for {
		var reply JoinReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Join request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Join response to S%d reply %+v", ck.clientId, ck.leaderId, reply)
		if ok && !reply.WrongLeader {
			return
		}
		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = svrid
				return
			}
		}
		/*reply := ck.tryEachServer(OP_JOIN, args)
		if !reply.(JoinReply).WrongLeader {
			return
		}*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	for {
		var reply LeaveReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Leave request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Leave response to S%d reply %+v", ck.clientId, ck.leaderId, reply)
		if ok && !reply.WrongLeader {
			return
		}

		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = svrid
				return
			}
		}
		/*reply := ck.tryEachServer(OP_LEAVE, args)
		if !reply.(LeaveReply).WrongLeader {
			return
		}*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	ck.currentSeqId = ck.snowflake.NextID()
	args.ClientId = ck.clientId
	args.SeqId = ck.currentSeqId

	for {
		var reply MoveReply
		raft.LogPrint(raft.INFO, dScClient, "C%d send Move request to S%d args %+v", ck.clientId, ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive Move response to S%d reply %+v", ck.clientId, ck.leaderId, reply)
		if ok && !reply.WrongLeader {
			return
		}
		// try each known server.
		for svrid, srv := range ck.servers {
			if svrid == ck.leaderId {
				continue
			}
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = svrid
				return
			}
		}

		/*reply := ck.tryEachServer(OP_MOVE, args)
		if !reply.(MoveReply).WrongLeader {
			return
		}*/
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) tryEachServer(opcode int, args interface{}) interface{} {
	svcMethPrefix := "ShardCtrler."
	var svcMeth string
	var reply interface{}
	switch opcode {
	case OP_QUERY:
		svcMeth = svcMethPrefix + "Query"
		reply = QueryReply{}
	case OP_JOIN:
		svcMeth = svcMethPrefix + "Join"
		reply = JoinReply{}
	case OP_LEAVE:
		svcMeth = svcMethPrefix + "Leave"
		reply = LeaveReply{}
	case OP_MOVE:
		svcMeth = svcMethPrefix + "Move"
		reply = MoveReply{}
	}

	op := svcMeth[len(svcMethPrefix):]

	raft.LogPrint(raft.INFO, dScClient, "C%d send %s request to S%d args %+v", ck.clientId, op, ck.leaderId, args)
	ok := ck.servers[ck.leaderId].Call(svcMeth, args, &reply)
	raft.LogPrint(raft.INFO, dScClient, "C%d receive %s response to S%d reply %+v", ck.clientId, op, ck.leaderId, reply)
	wrongLeader := ck.getFieldWrongLeader(reply, "WrongLeader")
	if ok && !wrongLeader {
		return reply
	}

	// try each known server.
	for srvid, srv := range ck.servers {
		raft.LogPrint(raft.INFO, dScClient, "C%d send %s request to S%d args %+v", ck.clientId, op, srvid, args)
		ok := srv.Call(svcMeth, args, &reply)
		raft.LogPrint(raft.INFO, dScClient, "C%d receive %s response to S%d reply %+v", ck.clientId, op, srvid, reply)
		wrongLeader = ck.getFieldWrongLeader(reply, "WrongLeader")
		if ok && !wrongLeader {
			ck.leaderId = srvid
			return reply
		}
	}

	return reply
}

func (ck *Clerk) getFieldWrongLeader(reply interface{}, fieldName string) bool {
	value := reflect.ValueOf(reply)
	//value = value.Elem() // ptr

	field := value.FieldByName(fieldName)
	return field.Bool()
}
