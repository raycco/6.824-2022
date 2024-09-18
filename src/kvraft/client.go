package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/snowflake"
)

const RetryInterval = 10 * time.Millisecond
const RequestTimeout = 4000 * time.Millisecond

var GlobalClientId int64 = 0
var GlobalSeqId int64 = 1
var mu sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
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
	// You'll have to add code here.
	ck.clientId = GlobalClientId
	GlobalClientId++
	ck.leaderId = 0
	var err error
	ck.snowflake, err = snowflake.NewSnowflake(ck.clientId)
	if err != nil {
		panic(err)
	}
	ck.currentSeqId = ck.snowflake.NextID()
	/*mu.Lock()
	ck.currentSeqId = GlobalSeqId
	GlobalSeqId++
	mu.Unlock()*/
	return ck
}

func (ck *Clerk) tryNextSever() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	time.Sleep(RetryInterval)
}

func (ck *Clerk) processReply(op string, replyCh chan Reply) (string, bool) {
	value := ""
	ok := false

	select {
	case reply := <-replyCh:
		raft.LogPrint(raft.INFO, dKvClient, "C%d recv %s response from S%d %+v", ck.clientId, op, ck.leaderId, reply)
		if reply.ok {
			if reply.Err == OK || reply.Err == ErrNoKey {
				value = reply.Value
				ck.currentSeqId = ck.snowflake.NextID()
				/*mu.Lock()
				ck.currentSeqId = GlobalSeqId
				GlobalSeqId++
				mu.Unlock()*/
				ok = true
			} else {
				ck.tryNextSever()
			}
		} else {
			ck.tryNextSever()
		}
	case <-time.After(RequestTimeout):
		raft.LogPrint(raft.INFO, dKvClient, "C%d recv %s response from S%d timeout", ck.clientId, op, ck.leaderId)
		ck.tryNextSever()
	}
	return value, ok
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if key != "" {
		var value string
		args := GetArgs{key, ck.clientId, ck.currentSeqId}

		replyCh := make(chan Reply)

		for {
			go func(peer int, args *GetArgs) {
				raft.LogPrint(raft.INFO, dKvClient, "C%d send Get request S%d %+v", ck.clientId, peer, args)
				var reply GetReply
				ok := ck.servers[peer].Call("KVServer.Get", args, &reply)
				replyCh <- Reply{ok, reply.Err, reply.Value}
			}(ck.leaderId, &args)

			var ok bool
			value, ok = ck.processReply("Get", replyCh)
			if ok {
				break
			}
		}
		return value

	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{key, value, op, ck.clientId, ck.currentSeqId}
	replyCh := make(chan Reply)

	for {

		/*ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		raft.LogPrint(raft.INFO, "KVCL", "C%d recv put/append response S%d ok=%t %+v %+v", ck.clientId, ck.leaderId, ok, args, reply)
		if ok {
			if reply.Err == OK {
				ck.currentSeqId = ck.snowflake.NextID()
				break
			} else {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				time.Sleep(5 * time.Millisecond)
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(5 * time.Millisecond)
		}*/

		go func(peer int, args *PutAppendArgs) {
			raft.LogPrint(raft.INFO, dKvClient, "C%d send Put/Append request S%d %+v", ck.clientId, peer, args)
			var reply PutAppendReply
			ok := ck.servers[peer].Call("KVServer.PutAppend", args, &reply)
			replyCh <- Reply{ok, reply.Err, ""}
		}(ck.leaderId, &args)

		var ok bool
		_, ok = ck.processReply("Put/Append", replyCh)
		if ok {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
