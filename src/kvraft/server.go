package kvraft

import (
	"bytes"
	"log"
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
	Opcode   int
	ClientId int64
	SeqId    int64
	Key      string
	Value    string
}

type OpReply struct {
	Err   Err
	Value string
}

type OpCache struct {
	Term    int
	Index   int
	OpRtn   OpReply
	ReplyCh chan int64
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

	ops       map[int64]*OpCache // sequence id -> op
	clientseq map[int64]int64    // client -> sequence id

	lastIncludedIndex int
	lastRaftStateSize int
}

func (kv *KVServer) processRequest(op Op) OpReply {

	raft.LogPrint(raft.DEBUG, dKvServer, "S%d process request op=%v ops=%v clientseq=%v", kv.me, op, kv.ops, kv.clientseq)
	var opReply OpReply
	_, isleader := kv.rf.GetState()
	if !isleader {
		opReply.Err = ErrWrongLeader
		opReply.Value = ""
		return opReply
	}

	/*if seqid := kv.clientseq[op.ClientId]; seqid != op.SeqId {
		delete(kv.ops, seqid)
		kv.clientseq[op.ClientId] = op.SeqId
	}*/

	opCache, ok := kv.ops[op.SeqId]
	if ok && len(opCache.OpRtn.Err) > 0 {
		opReply.Err = opCache.OpRtn.Err
		opReply.Value = opCache.OpRtn.Value
	} else {
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			opReply.Err = ErrWrongLeader
			opReply.Value = ""
		} else {
			opCache = &OpCache{term, index, OpReply{"", ""}, make(chan int64)}
			kv.ops[op.SeqId] = opCache

			kv.mu.Unlock()
			seqId := <-opCache.ReplyCh
			kv.mu.Lock()

			_, ok = kv.ops[seqId]
			if ok {
				opReply.Err = kv.ops[seqId].OpRtn.Err
				opReply.Value = kv.ops[seqId].OpRtn.Value
			} else {
				opReply.Err = ErrOutOfOrder
				opReply.Value = ""
			}
		}
	}

	return opReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OP_GET, args.ClientId, args.SeqId, args.Key, ""}
	raft.LogPrint(raft.INFO, dKvServer, "S%d recv Get request from C%d ops len=%d op=%+v", kv.me, args.ClientId, len(kv.ops), op)

	opReply := kv.processRequest(op)
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	raft.LogPrint(raft.INFO, dKvServer, "S%d send Get response to C%d reply %+v", kv.me, args.ClientId, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{-1, args.ClientId, args.SeqId, args.Key, args.Value}
	if args.Op == "Put" {
		op.Opcode = OP_PUT
	} else {
		op.Opcode = OP_APPEND
	}

	raft.LogPrint(raft.INFO, dKvServer, "S%d recv Put/Append request from C%d ops len=%d op=%+v", kv.me, args.ClientId, len(kv.ops), op)

	opReply := kv.processRequest(op)
	reply.Err = opReply.Err

	raft.LogPrint(raft.INFO, dKvServer, "S%d send Put/Append response to C%d reply %v", kv.me, args.ClientId, reply)
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
	kv.mu.Lock()
	raft.LogPrint(raft.DEBUG, dKvServer, "S%d kvdatabase %+v", kv.me, kv.kvdatabase)
	kv.mu.Unlock()
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

func (kv *KVServer) processOp(op Op, index int) {

	term, isLeader := kv.rf.GetState()
	var opReply OpReply

	opCache, ok := kv.ops[op.SeqId]
	raft.LogPrint(raft.INFO, dKvServer, "S%d T=%d I=%d Ldr=%t recv apply msg op = %+v cache = %+v",
		kv.me, term, index, isLeader, op, opCache)

	if ok {
		if len(opCache.OpRtn.Err) <= 0 {
			opReply = kv.opExecute(op)
			//kv.ops[op.SeqId] = &OpCache{opCache.term, opCache.index, op, opReply, opCache.replyCh}
			//kv.ops[op.SeqId].opR = opReply
			opCache.OpRtn = opReply
		}
	} else {
		opReply = kv.opExecute(op)
		opCache = &OpCache{term, index, opReply, nil}
		kv.ops[op.SeqId] = opCache
	}

	if seqId := kv.clientseq[op.ClientId]; seqId < op.SeqId {
		delete(kv.ops, seqId)
		kv.clientseq[op.ClientId] = op.SeqId
	}

	if isLeader {
		if term == opCache.Term && index == opCache.Index && opCache.ReplyCh != nil {
			kv.mu.Unlock()
			opCache.ReplyCh <- op.SeqId
			kv.mu.Lock()
			close(opCache.ReplyCh)
			opCache.ReplyCh = nil
		}
	}

	kv.lastIncludedIndex = index
}

func (kv *KVServer) applier() {
	for !kv.killed() {

		applyMsg := <-kv.applyCh

		kv.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.processOp(op, applyMsg.CommandIndex)
			kv.createSnapshot()
		} else if applyMsg.SnapshotValid {
			kv.ingestSnapshot(applyMsg.Snapshot, applyMsg.SnapshotIndex)
			kv.lastRaftStateSize = kv.rf.GetRaftStateSize()
		} else {
			// Ignore other types of ApplyMsg.
			raft.LogPrint(raft.WARN, dKvServer, "S%d recv unknown type apply msg %v", kv.me, applyMsg)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) ingestSnapshot(snapshot []byte, index int) {
	if snapshot == nil {
		raft.LogPrint(raft.ERROR, dKvServer, "S%d snapshot is nil", kv.me)
		return
	}

	byteBuffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(byteBuffer)
	var lastIncludedIndex int
	var kvdatabase map[string]string
	var clientseq map[int64]int64
	var ops map[int64]*OpCache
	if decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&kvdatabase) != nil ||
		decoder.Decode(&clientseq) != nil ||
		decoder.Decode(&ops) != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "S%d snapshot Decode() error", kv.me)
		return
	}
	if index != -1 && index != lastIncludedIndex {
		raft.LogPrint(raft.ERROR, dKvServer, "S%d snapshot doesn't match m.SnapshotIndex", kv.me)
		return
	}

	raft.LogPrint(raft.DEBUG, dKvServer, "S%d ingest ops = %+v clientseq = %+v", kv.me, ops, clientseq)

	for clientid, seqid := range clientseq {
		_, ok := kv.clientseq[clientid]
		if !ok {
			kv.clientseq[clientid] = seqid
		} else {
			if kv.clientseq[clientid] < seqid {
				kv.clientseq[clientid] = seqid
			}
		}
	}

	for _, seqid := range kv.clientseq {
		opCache, ok := ops[seqid]
		if ok {
			kv.ops[seqid] = opCache
		}
	}
	raft.LogPrint(raft.DEBUG, dKvServer, "S%d ingest kv.ops = %+v kv.clientseq = %+v", kv.me, kv.ops, kv.clientseq)

	// apply message may apply one more index when create snapshot, one index apply two times
	// 1. current seqid => op has not executed in snapshot, the first time execute may not write to db
	// 2. current seqid => op has executed one time in snapshot, so no need to execute second time
	for lastApplied := lastIncludedIndex + 1; lastApplied <= kv.lastIncludedIndex; lastApplied++ {
		for seqid, opCache := range kv.ops {
			_, ok := ops[seqid]
			if !ok {
				opCache.OpRtn = OpReply{"", ""}
			}
		}
	}

	kv.lastIncludedIndex = lastIncludedIndex
	kv.kvdatabase = kvdatabase

	raft.LogPrint(raft.INFO, dKvServer, "S%d ingest snapshot LII=%d", kv.me, kv.lastIncludedIndex)
	raft.LogPrint(raft.DEBUG, dKvServer, "S%d ingest snapshot kvdatabase = %+v", kv.me, kv.kvdatabase)
}

func (kv *KVServer) createSnapshot() {
	if kv.maxraftstate != -1 {
		raftStateSize := kv.rf.GetRaftStateSize()
		if raftStateSize-kv.lastRaftStateSize >= kv.maxraftstate {
			raft.LogPrint(raft.INFO, dKvServer, "S%d RSS=%d LRSS=%d LII=%d",
				kv.me, raftStateSize, kv.lastRaftStateSize, kv.lastIncludedIndex)
			raft.LogPrint(raft.DEBUG, dKvServer, "S%d kvdatabase = %+v", kv.me, kv.kvdatabase)
			raft.LogPrint(raft.DEBUG, dKvServer, "S%d ops = %+v clientseq = %+v", kv.me, kv.ops, kv.clientseq)

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			encoder.Encode(kv.lastIncludedIndex)
			encoder.Encode(kv.kvdatabase)
			encoder.Encode(kv.clientseq)
			encoder.Encode(kv.ops)
			kv.rf.Snapshot(kv.lastIncludedIndex, byteBuffer.Bytes())
		}
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

	kv.ops = make(map[int64]*OpCache)
	kv.clientseq = make(map[int64]int64)

	kv.lastIncludedIndex = 0
	kv.lastRaftStateSize = kv.rf.GetRaftStateSize()

	go kv.applier()

	return kv
}
