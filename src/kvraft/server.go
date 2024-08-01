package kvraft

import (
	"bytes"
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
	term    int
	index   int
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

	ops       map[int64]*OpCache // sequence id -> op
	sequences map[int64]int64    // client -> sequence id

	lastIncludedIndex int
	lastIncludedTerm  int
	lastRaftStateSize int
}

func isElementInSlice(slice []int64, target int64) bool {
	for _, element := range slice {
		if element == target {
			return true
		}
	}
	return false
}

func (kv *KVServer) pureCache() {

	lastSeqIds := make([]int64, len(kv.sequences))
	for _, value := range kv.sequences {
		lastSeqIds = append(lastSeqIds, value)
	}

	delSeqIds := make([]int64, 0)
	for key := range kv.ops {
		if !isElementInSlice(lastSeqIds, key) {
			delSeqIds = append(delSeqIds, key)
		}
	}

	for _, seqid := range delSeqIds {
		/*if kv.ops[seqid].replyCh != nil {
			close(kv.ops[seqid].replyCh) // close cause receiver return
		}*/
		delete(kv.ops, seqid)
	}
}

func (kv *KVServer) processRequest(op Op) OpReply {

	raft.LogPrint(raft.DEBUG, "KVSR", "S%d process request op=%v ops=%v sequences=%v", kv.me, op, kv.ops, kv.sequences)
	if seqid := kv.sequences[op.ClientId]; seqid != op.SeqId {
		delete(kv.ops, seqid)
		kv.sequences[op.ClientId] = op.SeqId
	}

	var opReply OpReply
	opCache, ok := kv.ops[op.SeqId]
	if ok && reflect.DeepEqual(op, opCache.op) && len(opCache.opR.Err) > 0 {
		opReply.Err = opCache.opR.Err
		opReply.Value = opCache.opR.Value
	} else {
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			opReply.Err = ErrWrongLeader
			opReply.Value = ""
		} else {
			opCache = &OpCache{term, index, op, OpReply{"", ""}, make(chan int64)}
			kv.ops[op.SeqId] = opCache

			kv.mu.Unlock()
			seqId := <-opCache.replyCh
			kv.mu.Lock()

			_, ok = kv.ops[seqId]
			if ok {
				opReply.Err = kv.ops[seqId].opR.Err
				opReply.Value = kv.ops[seqId].opR.Value
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
	raft.LogPrint(raft.INFO, "KVSR", "S%d recv Get request from C%d ops len=%d op=%+v", kv.me, args.ClientId, len(kv.ops), op)

	//term, isLeader := kv.rf.GetState()
	//kv.pureCache() // crash and then restart, kv.sequences is null, can not pure cache at the time

	/*opCache, ok := kv.ops[args.SeqId]
	if ok && reflect.DeepEqual(op, opCache.op) && len(opCache.opR.Err) > 0 {
		reply.Err = opCache.opR.Err
		reply.Value = opCache.opR.Value
	} else {
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.Value = ""
		} else {
			opCache = OpCache{term, index, op, OpReply{"", ""}, make(chan int64)}
			kv.ops[args.SeqId] = opCache

			kv.mu.Unlock()
			seqId := <-opCache.replyCh
			kv.mu.Lock()

			_, ok = kv.ops[seqId]
			if ok {
				reply.Err = kv.ops[seqId].opR.Err
				reply.Value = kv.ops[seqId].opR.Value
			} else {
				reply.Err = ErrOutOfOrder
				reply.Value = ""
			}
		}
	}*/
	opReply := kv.processRequest(op)
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	raft.LogPrint(raft.INFO, "KVSR", "S%d send Get response to C%d reply %+v", kv.me, args.ClientId, reply)
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

	raft.LogPrint(raft.INFO, "KVSR", "S%d recv Put/Append request from C%d ops len=%d op=%+v", kv.me, args.ClientId, len(kv.ops), op)

	//term, isLeader := kv.rf.GetState()
	//kv.pureCache() // crash and then restart, kv.sequences is null, can not pure cat the time

	/*opCache, ok := kv.ops[args.SeqId]
	if ok && reflect.DeepEqual(op, opCache.op) && len(opCache.opR.Err) > 0 {
		reply.Err = opCache.opR.Err
	} else {
		index, term, isLeader := kv.rf.Start(op)

		if !isLeader {
			reply.Err = ErrWrongLeader
		} else {
			opCache = OpCache{term, index, op, OpReply{"", ""}, make(chan int64)}
			kv.ops[args.SeqId] = opCache

			raft.LogPrint(raft.INFO, "KVSR", "S%d recv Put/Append request from C%d cache len=%d %+v", kv.me, args.ClientId, len(kv.ops), opCache)

			kv.mu.Unlock()
			seqId := <-opCache.replyCh
			kv.mu.Lock()

			_, ok = kv.ops[seqId]
			if ok {
				reply.Err = kv.ops[seqId].opR.Err
			} else {
				reply.Err = ErrOutOfOrder
			}
		}
	}*/
	opReply := kv.processRequest(op)
	reply.Err = opReply.Err

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

func (kv *KVServer) processOp(op Op, index int) {

	term, isLeader := kv.rf.GetState()
	var opReply OpReply
	opCache, ok := kv.ops[op.SeqId]
	raft.LogPrint(raft.INFO, "KVSR", "S%d T=%d Leader=%t recv apply msg ok=%t %+v", kv.me, term, isLeader, ok, opCache)
	if ok {
		if reflect.DeepEqual(opCache.op, op) {
			if len(opCache.opR.Err) <= 0 {
				opReply = kv.opExecute(op)
				//kv.ops[op.SeqId] = &OpCache{opCache.term, opCache.index, op, opReply, opCache.replyCh}
				//kv.ops[op.SeqId].opR = opReply
				opCache.opR = opReply
			} else {
				opReply = opCache.opR
			}
		} else {
			opReply.Err = ErrNoAgreement // todo
		}
	} else {
		opReply = kv.opExecute(op)
		if seqid := kv.sequences[op.ClientId]; seqid != op.SeqId {
			delete(kv.ops, seqid)
			kv.sequences[op.ClientId] = op.SeqId
		}
		opCache = &OpCache{term, index, op, opReply, nil}
		kv.ops[op.SeqId] = opCache
	}

	if isLeader {
		if term == opCache.term && opCache.replyCh != nil {
			kv.mu.Unlock()
			opCache.replyCh <- opCache.op.SeqId
			kv.mu.Lock()
			close(opCache.replyCh)
			opCache.replyCh = nil
		}
	}

	kv.lastIncludedIndex = index
	kv.lastIncludedTerm = term

	/*if isLeader {
		kv.mu.Unlock()
		if opCache.term == term && opCache.replyCh != nil {
			opCache.replyCh <- opCache.op.SeqId
		}
		kv.mu.Lock()
	}*/
}

func (kv *KVServer) applier() {
	for !kv.killed() {

		applyMsg := <-kv.applyCh
		raft.LogPrint(raft.INFO, "KVSR", "S%d recv apply msg %v", kv.me, applyMsg)

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
			raft.LogPrint(raft.WARN, "KVSR", "S%d recv unknown type apply msg %v", kv.me, applyMsg)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) ingestSnapshot(snapshot []byte, index int) {
	if snapshot == nil {
		raft.LogPrint(raft.ERROR, "KVSR", "S%d snapshot is nil", kv.me)
		return
	}

	byteBuffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(byteBuffer)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var kvdatabase map[string]string
	if decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil ||
		decoder.Decode(&kvdatabase) != nil {
		raft.LogPrint(raft.ERROR, "KVSR", "S%d snapshot Decode() error", kv.me)
		return
	}
	if index != -1 && index != lastIncludedIndex {
		raft.LogPrint(raft.ERROR, "KVSR", "S%d snapshot doesn't match m.SnapshotIndex", kv.me)
		return
	}

	// apply message may apply one more index after create snapshot
	for lastApplied := lastIncludedIndex + 1; lastApplied <= kv.lastIncludedIndex; lastApplied++ {
		for _, opCache := range kv.ops {
			if opCache.index == lastApplied {
				opCache.opR = OpReply{"", ""}
			}
		}
	}

	kv.lastIncludedIndex = lastIncludedIndex
	kv.lastIncludedTerm = lastIncludedTerm
	kv.kvdatabase = kvdatabase

	raft.LogPrint(raft.INFO, "KVSR", "S%d ingest snapshot LII=%d LIT=%d", kv.me, kv.lastIncludedIndex, kv.lastIncludedTerm)
	raft.LogPrint(raft.DEBUG, "KVSR", "S%d ingest snapshot kvdatabase = %+v", kv.me, kv.kvdatabase)
}

func (kv *KVServer) createSnapshot() {
	if kv.maxraftstate != -1 {
		raftStateSize := kv.rf.GetRaftStateSize()
		if raftStateSize-kv.lastRaftStateSize >= kv.maxraftstate {
			raft.LogPrint(raft.INFO, "KVSR", "S%d RSS=%d LRSS=%d LII=%d LIT=%d",
				kv.me, raftStateSize, kv.lastRaftStateSize, kv.lastIncludedIndex, kv.lastIncludedTerm)
			raft.LogPrint(raft.DEBUG, "KVSR", "S%d kvdatabase = %+v", kv.me, kv.kvdatabase)

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			encoder.Encode(kv.lastIncludedIndex)
			encoder.Encode(kv.lastIncludedTerm)
			encoder.Encode(kv.kvdatabase)
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
	kv.sequences = make(map[int64]int64)

	kv.lastIncludedIndex = 0
	kv.lastIncludedTerm = -1
	kv.lastRaftStateSize = kv.rf.GetRaftStateSize()

	go kv.applier()

	return kv
}
