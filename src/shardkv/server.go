package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	OP_GET     = 1
	OP_PUT     = 2
	OP_APPEND  = 3
	OP_MIGRATE = 4
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
	Cfg      interface{}
}

type OpReply struct {
	Err   Err
	Value string
}

type OpCache struct {
	Term   int
	Index  int
	Opcode int
	SeqId  int64
	IsExec bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead     int32
	database map[string]string

	cfgck  *shardctrler.Clerk
	config shardctrler.Config

	clientop map[int64]*OpCache   // client id -> last op
	replyChs map[int]chan OpReply // index -> reply channel

	lastIncludedIndex int
	lastRaftStateSize int

	migrateCond  *sync.Cond
	migrateCond1 *sync.Cond
	migrateCh    chan int
	migratingDb  map[string]string

	migratingGidConfig map[int]shardctrler.Config

	migratingCond *sync.Cond
	cfgUpdateCond *sync.Cond

	waitPushCount map[int]int // wait push count for each config version

	pushed bool
	pulled bool
	state  int

	lastPushSeqId int64
	lastPullSeqId int64

	logPrefix string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OP_GET, args.ClientId, args.SeqId, args.Key, "", nil}
	raft.LogPrint(raft.INFO, dKvServer, "%s recv Get request from C%d op=%+v", kv.logPrefix, args.ClientId, op)

	opReply := kv.processRequest(op)
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	raft.LogPrint(raft.INFO, dKvServer, "%s send Get response to C%d reply %+v", kv.logPrefix, args.ClientId, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{-1, args.ClientId, args.SeqId, args.Key, args.Value, nil}
	if args.Op == "Put" {
		op.Opcode = OP_PUT
	} else {
		op.Opcode = OP_APPEND
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s recv Put/Append request from C%d op=%+v", kv.logPrefix, args.ClientId, op)

	opReply := kv.processRequest(op)
	reply.Err = opReply.Err

	raft.LogPrint(raft.INFO, dKvServer, "%s send Put/Append response to C%d reply %v", kv.logPrefix, args.ClientId, reply)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	raft.LogPrint(raft.INFO, dKvServer, "%s killed", kv.logPrefix)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.cfgck = shardctrler.MakeClerk(kv.ctrlers)
	// the latest config when restart, ensure the kv not in snapshot save to db by apply message
	//kv.config = kv.cfgck.Query(-1)

	kv.logPrefix = fmt.Sprintf("G%d-S%d-C%d", kv.gid, kv.me, kv.cfgck.GetClientId())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.database = make(map[string]string)
	kv.clientop = make(map[int64]*OpCache)
	kv.replyChs = make(map[int]chan OpReply)

	kv.lastIncludedIndex = 0
	kv.lastRaftStateSize = kv.rf.GetRaftStateSize()

	kv.migratingDb = make(map[string]string)
	kv.migratingGidConfig = make(map[int]shardctrler.Config)
	kv.state = SERVING

	go kv.applier()

	go kv.ticker()

	kv.migrateCond = sync.NewCond(&kv.mu)
	kv.migrateCond1 = sync.NewCond(&kv.mu)
	kv.migratingCond = sync.NewCond(&kv.mu)
	kv.cfgUpdateCond = sync.NewCond(&kv.mu)
	kv.migrateCh = make(chan int)

	kv.waitPushCount = make(map[int]int)

	kv.lastPushSeqId = 0
	kv.lastPullSeqId = 0

	return kv
}

func (kv *ShardKV) waitForMigrate(shard int, clientid int64, key string) {
	for {
		wait := false
		if kv.config.Num > 1 {
			lastCfg := kv.cfgck.Query(kv.config.Num - 1)
			gid := lastCfg.Shards[shard]
			if kv.state == MIGRATING && len(kv.migratingGidConfig) > 0 {
				_, ok := kv.migratingGidConfig[gid]
				_, exist := kv.migratingDb[key]
				if (!ok && !exist) || (ok && exist) {
					wait = true
				}
			}
		}

		if kv.state != WAIT && (clientid == PUSH_CLIENT_ID || !wait) {
			break
		}
		kv.migratingCond.Wait()
	}
}

func (kv *ShardKV) processRequest(op Op) OpReply {

	raft.LogPrint(raft.INFO, dKvServer, "%s process request config num = %d, op %+v",
		kv.logPrefix, kv.config.Num, op)
	shard := key2shard(op.Key)
	gid := kv.config.Shards[shard]
	if gid != kv.gid {
		return OpReply{ErrWrongGroup, ""}
	}

	kv.waitForMigrate(shard, op.ClientId, op.Key)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return OpReply{ErrWrongLeader, ""}
	}

	/*if op.Opcode != OP_GET {
		opCache, ok := kv.clientop[op.ClientId]
		if ok && op.SeqId < opCache.SeqId {
			return OpReply{OK, ""}
		}
	}*/

	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		return OpReply{ErrWrongLeader, ""}
	} else {
		opCache := &OpCache{term, index, op.Opcode, op.SeqId, false}
		kv.clientop[op.ClientId] = opCache

		replyCh := make(chan OpReply)
		kv.replyChs[index] = replyCh

		kv.mu.Unlock()
		opReply := <-replyCh
		raft.LogPrint(raft.INFO, dKvServer, "%s send response to C%d %+v", kv.logPrefix, op.ClientId, opReply)
		kv.mu.Lock()
		return opReply
	}
}

func (kv *ShardKV) opExecute(op Op) OpReply {
	var opReply OpReply

	shard := key2shard(op.Key)
	gid := kv.config.Shards[shard]
	if gid != kv.gid {
		return OpReply{ErrWrongGroup, ""}
	}

	switch op.Opcode {
	case OP_GET:
		value, ok := kv.database[op.Key]
		if ok {
			opReply.Err = OK
			opReply.Value = value
		} else {
			opReply.Err = ErrNoKey
			opReply.Value = ""
		}
	case OP_PUT:
		kv.database[op.Key] = op.Value
		opReply.Err = OK
		opReply.Value = kv.database[op.Key]
	case OP_APPEND:
		kv.database[op.Key] += op.Value
		opReply.Err = OK
		opReply.Value = kv.database[op.Key]
	}

	return opReply
}

func (kv *ShardKV) processClientOp(op Op, term int, index int) OpReply {

	var opReply OpReply
	clientid := op.ClientId
	seqid := op.SeqId

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if op.Opcode == OP_GET {
			opReply = kv.opExecute(op)
			if seqid >= opCache.SeqId {
				opCache = &OpCache{term, index, op.Opcode, seqid, false}
			}
		} else {
			if (seqid >= opCache.SeqId && index >= opCache.Index) || !opCache.IsExec {
				opReply = kv.opExecute(op)
				opCache = &OpCache{term, index, op.Opcode, seqid, true}
			} else {
				opReply = OpReply{OK, ""}
			}
		}
	} else {
		opReply = kv.opExecute(op)
		if op.Opcode == OP_GET {
			opCache = &OpCache{term, index, op.Opcode, seqid, false}
		} else {
			opCache = &OpCache{term, index, op.Opcode, seqid, true}
		}
	}
	kv.clientop[clientid] = opCache
	return opReply
}

func (kv *ShardKV) processOp(op Op, index int) {
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process op %+v clientop %v",
		kv.logPrefix, index, kv.config.Num, op, kv.clientop)

	term, isLeader := kv.rf.GetState()

	var opReply OpReply
	if op.Opcode == OP_MIGRATE {
		opReply = kv.processMigrateOp(op, term, index, isLeader)
	} else {
		opReply = kv.processClientOp(op, term, index)
	}

	if isLeader {
		if replyCh, ok := kv.replyChs[index]; ok && replyCh != nil {
			kv.mu.Unlock()
			replyCh <- opReply
			kv.mu.Lock()

			close(replyCh)
			replyCh = nil
			delete(kv.replyChs, index)
		}
	}

	kv.lastIncludedIndex = index
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d process op reply %v", kv.logPrefix, index, opReply)
}

func (kv *ShardKV) applier() {
	for !kv.killed() {

		applyMsg := <-kv.applyCh

		kv.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.processOp(op, applyMsg.CommandIndex)
			kv.createSnapshot(false)
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

func (kv *ShardKV) ingestSnapshot(snapshot []byte, index int) {
	if snapshot == nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot is nil", kv.logPrefix)
		return
	}

	byteBuffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(byteBuffer)
	var lastIncludedIndex int
	var database map[string]string
	var clientop map[int64]*OpCache
	var config shardctrler.Config
	//var state int
	if decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&database) != nil ||
		decoder.Decode(&clientop) != nil ||
		decoder.Decode(&config) != nil { /*||
		decoder.Decode(&state) != nil */
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot Decode() error", kv.logPrefix)
		return
	}
	if index != -1 && index != lastIncludedIndex {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot doesn't match m.SnapshotIndex", kv.logPrefix)
		return
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s ingest clientop %+v", kv.logPrefix, clientop)

	for clientid, op := range clientop {
		_, ok := kv.clientop[clientid]
		if !ok {
			kv.clientop[clientid] = op
		} else {
			if kv.clientop[clientid].SeqId < op.SeqId {
				kv.clientop[clientid] = op
			}
		}
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s ingest kv.clientop %+v", kv.logPrefix, kv.clientop)

	// apply message may apply one more index when create snapshot, one index apply two times
	// 1. current seqid => op was not executed in snapshot
	// 2. current seqid => op has executed one time in snapshot
	for lastApplied := lastIncludedIndex + 1; lastApplied <= kv.lastIncludedIndex; lastApplied++ {
		for clientid, op := range kv.clientop {
			_, ok := clientop[clientid]
			if !ok {
				op.IsExec = false
			}
		}
	}

	kv.lastIncludedIndex = lastIncludedIndex
	kv.config = config
	//kv.state = state
	for key, value := range database {
		// may receive migrate data, so can not use assignment directly
		kv.database[key] = value
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s ingest kv.config %+v", kv.logPrefix, kv.config)

	raft.LogPrint(raft.INFO, dKvServer, "%s ingest snapshot LII=%d", kv.logPrefix, kv.lastIncludedIndex)
	raft.LogPrint(raft.INFO, dKvServer, "%s ingest snapshot database = %+v", kv.logPrefix, kv.database)
}

func (kv *ShardKV) createSnapshot(force bool) {
	if kv.maxraftstate != -1 {
		raftStateSize := kv.rf.GetRaftStateSize()
		if raftStateSize-kv.lastRaftStateSize >= kv.maxraftstate || force {
			raft.LogPrint(raft.INFO, dKvServer, "%s RSS=%d LRSS=%d LII=%d",
				kv.logPrefix, raftStateSize, kv.lastRaftStateSize, kv.lastIncludedIndex)
			raft.LogPrint(raft.INFO, dKvServer, "%s database = %+v", kv.logPrefix, kv.database)
			raft.LogPrint(raft.INFO, dKvServer, "%s clientop = %+v", kv.logPrefix, kv.clientop)

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			encoder.Encode(kv.lastIncludedIndex)
			encoder.Encode(kv.database)
			encoder.Encode(kv.clientop)
			encoder.Encode(kv.config)
			//encoder.Encode(kv.state)
			kv.rf.Snapshot(kv.lastIncludedIndex, byteBuffer.Bytes())
		}
	}
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {

		kv.mu.Lock()

		_, isLeader := kv.rf.GetState()
		if isLeader {
			for {
				if len(kv.migratingDb) <= 0 && kv.state == SERVING {
					break
				}
				kv.cfgUpdateCond.Wait()
			}
			config := kv.cfgck.Query(-1)
			if config.Num > kv.config.Num {
				num := kv.config.Num + 1
				newcfg := kv.cfgck.Query(num)

				kv.state = MIGRATING

				op := Op{OP_MIGRATE, CFG_CLIENT_ID, int64(num), "", "", newcfg}
				raft.LogPrint(raft.INFO, dKvServer, "%s num = %d ticker op = %+v", kv.logPrefix, num, op)
				index, term, _ := kv.rf.Start(op)

				opCache := &OpCache{term, index, op.Opcode, op.SeqId, false}
				kv.clientop[op.ClientId] = opCache

				replyCh := make(chan OpReply)
				kv.replyChs[index] = replyCh

				kv.mu.Unlock()
				<-replyCh
				kv.migrateCond.Broadcast()
				kv.mu.Lock()

			} else {
				kv.config = config
				raft.LogPrint(raft.INFO, dKvServer, "%s ticker config = %+v", kv.logPrefix, kv.config)
			}
		}

		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}
