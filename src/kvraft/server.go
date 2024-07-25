package kvraft

import (
	"log"
	"reflect"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_GET    = 1
	OP_PUT    = 2
	OP_APPEND = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opcode int
	SeqId  int64
	Key    string
	Value  string
}

type OpReply struct {
	Err   Err
	Value string
}

type OpCache struct {
	term    int
	op      Op
	opR     OpReply
	replyCh chan int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdatabase map[string]string

	ops map[int64]OpCache

	getReplyCh chan OpReply
	putReplyCh chan OpReply

	replyCond *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OP_GET, args.SeqId, args.Key, ""}
	raft.LogPrint(raft.INFO, "KVSR", "S%d recv Get request from C%d %+v", kv.me, args.ClientId, op)

	//term, isLeader := kv.rf.GetState()
	term := -1
	isLeader := false
	opCache, ok := kv.ops[args.SeqId]
	if ok && reflect.DeepEqual(op, opCache.op) && len(opCache.opR.Err) > 0 {
		reply.Err = opCache.opR.Err
		reply.Value = opCache.opR.Value
		return
	} else {
		_, term, isLeader = kv.rf.Start(op)
		if isLeader {
			opCache.term = term
			opCache.op = op
			opCache.opR = OpReply{"", ""}
			opCache.replyCh = make(chan int64)
			kv.ops[args.SeqId] = opCache
		}
	}

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		//kv.replyCond.Wait()
		kv.mu.Unlock()
		seqId := <-opCache.replyCh
		kv.mu.Lock()

		_, ok = kv.ops[seqId]
		if ok {
			reply.Err = kv.ops[seqId].opR.Err
			reply.Value = kv.ops[seqId].opR.Value
		} else {
			reply.Err = "ErrReplyOrder"
		}
	}
	raft.LogPrint(raft.INFO, "KVSR", "S%d send Get response to C%d reply %+v", kv.me, args.ClientId, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{-1, args.SeqId, args.Key, args.Value}
	if args.Op == "Put" {
		op.Opcode = OP_PUT
	} else {
		op.Opcode = OP_APPEND
	}

	raft.LogPrint(raft.INFO, "KVSR", "S%d recv Put/Append request from C%d %+v", kv.me, args.ClientId, op)

	//term, isLeader := kv.rf.GetState()

	term := -1
	isLeader := false
	opCache, ok := kv.ops[args.SeqId]
	if ok && reflect.DeepEqual(op, opCache.op) && len(opCache.opR.Err) > 0 {
		reply.Err = opCache.opR.Err
		return
	} else {
		_, term, isLeader = kv.rf.Start(op)
		if isLeader {
			opCache.term = term
			opCache.op = op
			opCache.opR = OpReply{"", ""}
			opCache.replyCh = make(chan int64)
			kv.ops[args.SeqId] = opCache
		}
	}

	raft.LogPrint(raft.INFO, "KVSR", "S%d recv Put/Append request from C%d %+v", kv.me, args.ClientId, opCache)

	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		//kv.replyCond.Wait()
		kv.mu.Unlock()
		seqId := <-opCache.replyCh
		kv.mu.Lock()

		_, ok = kv.ops[seqId]
		if ok {
			reply.Err = kv.ops[seqId].opR.Err
		} else {
			reply.Err = "ErrReplyOrder"
		}
	}
	raft.LogPrint(raft.INFO, "KVSR", "S%d send Put/Append response to C%d reply %v", kv.me, args.ClientId, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) opExecute(op Op) OpReply {
	var opReply OpReply
	switch op.Opcode {
	case OP_GET:
		value, ok := kv.kvdatabase[op.Key]
		if ok {
			opReply.Err = OK
			opReply.Value = value
		} else {
			opReply.Err = ErrNoKey
			opReply.Value = ""
		}
	case OP_PUT:
		kv.kvdatabase[op.Key] = op.Value
		opReply.Err = OK
	case OP_APPEND:
		kv.kvdatabase[op.Key] += op.Value
		opReply.Err = OK
	}

	return opReply
}

func (kv *KVServer) applier() {
	for !kv.killed() {

		applyMsg := <-kv.applyCh
		raft.LogPrint(raft.INFO, "KVSR", "S%d recv apply msg %v", kv.me, applyMsg)

		kv.mu.Lock()
		//term, isLeader := kv.rf.GetState() // Get lock may cost time

		op := applyMsg.Command.(Op)
		var opReply OpReply

		opCache, ok := kv.ops[op.SeqId]
		raft.LogPrint(raft.INFO, "KVSR", "S%d T=%d Leader=%t recv apply msg ok=%t %+v",
			kv.me, applyMsg.CommandTerm, applyMsg.IsLeader, ok, opCache)
		if ok {
			if reflect.DeepEqual(opCache.op, op) {
				if len(opCache.opR.Err) <= 0 {
					opReply = kv.opExecute(op)
					kv.ops[op.SeqId] = OpCache{opCache.term, op, opReply, opCache.replyCh}
					//opCache.opR = opReply
				} else {
					opReply = opCache.opR
				}
			} else {
				opReply.Err = ErrNoAgreement // todo
			}
		} else {
			opReply = kv.opExecute(op)
			kv.ops[op.SeqId] = OpCache{opCache.term, op, opReply, nil}
		}

		if applyMsg.IsLeader {
			if applyMsg.CommandTerm == opCache.term && opCache.replyCh != nil {
				opCache.replyCh <- opCache.op.SeqId
			}
		}

		/*if isLeader {
			kv.mu.Unlock()
			if opCache.term == term && opCache.replyCh != nil {
				opCache.replyCh <- opCache.op.SeqId
			}
			kv.mu.Lock()
		}*/
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvdatabase = make(map[string]string)
	kv.getReplyCh = make(chan OpReply)
	kv.putReplyCh = make(chan OpReply)

	kv.ops = make(map[int64]OpCache)

	kv.replyCond = sync.NewCond(&kv.mu)

	go kv.applier()

	return kv
}
