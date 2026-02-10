package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

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
	OP_CONFIG  = 5
)

type KeyVal struct {
	Key   string
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opcode   int
	Num      int
	ClientId int64
	SeqId    int64
	Type     interface{}
}

type OpReply struct {
	Err   Err
	Value string
}

type OpCache struct {
	Term   int
	Index  int
	Opcode int
	Num    int
	SeqId  int64
	Err    Err
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
	dbstat   DbStat

	cfgck      *shardctrler.Clerk
	config     Cfg
	lastConfig Cfg

	clientop map[int64]*OpCache   // client id -> last op
	replyChs map[int]chan OpReply // index -> reply channel

	lastIncludedIndex int
	lastRaftStateSize int
	migrateStat       map[int]bool

	migrateCond  *sync.Cond
	migrateCond1 *sync.Cond
	migrateCh    chan int
	migratingDb  map[string]string

	migratingGidConfig map[int]Cfg

	migratingCond *sync.Cond
	cfgUpdateCond *sync.Cond

	waitPushCount map[int]int // wait push count for each config version

	lastPushSeqId int64
	lastPullSeqId int64

	logPrefix string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OP_GET, args.Num, args.ClientId, args.SeqId, KeyVal{args.Key, ""}}
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

	op := Op{-1, args.Num, args.ClientId, args.SeqId, KeyVal{args.Key, args.Value}}
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
	labgob.Register(Cfg{})
	labgob.Register(DbStat{})
	labgob.Register(KeyVal{})

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
	kv.migrateStat = make(map[int]bool)
	kv.migrateStat[0] = true

	kv.migratingDb = make(map[string]string)
	kv.migratingGidConfig = make(map[int]Cfg)

	kv.migrateCond = sync.NewCond(&kv.mu)
	kv.migrateCond1 = sync.NewCond(&kv.mu)
	kv.migratingCond = sync.NewCond(&kv.mu)
	kv.cfgUpdateCond = sync.NewCond(&kv.mu)
	kv.waitPushCount = make(map[int]int)

	kv.dbstat = kv.newDbStat()

	go kv.applier()

	go kv.configer()

	//go kv.migrater()

	return kv
}

func (kv *ShardKV) waitForMigrate(shard int, clientid int64, key string) {
	for {
		wait := false
		if kv.config.Num > 1 {
			//lastCfg := kv.cfgck.Query(kv.config.Num - 1)
			gid := kv.lastConfig.Shards[shard]
			if kv.dbstat.Stat == MIGRATING && len(kv.migratingGidConfig) > 0 {
				_, ok := kv.migratingGidConfig[gid]
				_, exist := kv.migratingDb[key]
				if (!ok && !exist) || (ok && exist) {
					wait = true
				}
			}
		}

		if clientid == MIGRATE_CLIENT_ID || (kv.dbstat.Stat != WAITING && !wait) {
			break
		}
		kv.migratingCond.Wait()
	}
}

func (kv *ShardKV) isWrongGroup(num int, gid int, shard int) bool {

	if num != kv.config.Num {
		return true
	}

	if gid != kv.gid {
		return true
	}

	switch kv.dbstat.Stat {
	case CONFIGING:
		return true
	case PUSHING:
		for _, shards := range kv.dbstat.DstGidShards {
			if containsShard(shards, shard) {
				return true
			}
		}
	case WAITING:
		for _, shards := range kv.dbstat.SrcGidShards {
			if containsShard(shards, shard) {
				return true
			}
		}
	}
	return false
}

func (kv *ShardKV) processRequest(op Op) OpReply {

	raft.LogPrint(raft.INFO, dKvServer, "%s process request config num = %d, op %+v",
		kv.logPrefix, kv.config.Num, op)

	key := op.Type.(KeyVal).Key

	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	if kv.isWrongGroup(op.Num, gid, shard) {
		return OpReply{ErrWrongGroup, ""}
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return OpReply{ErrWrongLeader, ""}
	}

	kv.waitForMigrate(shard, op.ClientId, key)

	if op.Opcode != OP_GET {
		opCache, ok := kv.clientop[op.ClientId]
		if ok && (op.SeqId < opCache.SeqId ||
			(op.SeqId == opCache.SeqId && opCache.Err == OK)) {
			return OpReply{OK, ""}
		}
	}

	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		return OpReply{ErrWrongLeader, ""}
	} else {
		opCache := &OpCache{term, index, op.Opcode, kv.config.Num, op.SeqId, Empty}
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

	key := op.Type.(KeyVal).Key
	value := op.Type.(KeyVal).Value

	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	if kv.gid != gid {
		return OpReply{ErrWrongGroup, ""}
	}

	switch op.Opcode {
	case OP_GET:
		_, ok := kv.database[key]
		if ok {
			opReply.Err = OK
			opReply.Value = kv.database[key]
		} else {
			opReply.Err = ErrNoKey
			opReply.Value = ""
		}
	case OP_PUT:
		kv.database[key] = value
		opReply.Err = OK
		opReply.Value = kv.database[key]
	case OP_APPEND:
		kv.database[key] += value
		opReply.Err = OK
		opReply.Value = kv.database[key]
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
				opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, Empty}
				kv.clientop[clientid] = opCache
			}
		} else {
			if (seqid > opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
				opReply = kv.opExecute(op)
				opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, opReply.Err}
				kv.clientop[clientid] = opCache
			} else {
				opReply = OpReply{OK, ""}
			}
		}
	} else {
		opReply = kv.opExecute(op)
		if op.Opcode == OP_GET {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, Empty}
		} else {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, opReply.Err}
		}
		kv.clientop[clientid] = opCache
	}

	return opReply
}

func (kv *ShardKV) processOp(op Op, index int) {
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process op %+v clientop %v",
		kv.logPrefix, index, kv.config.Num, op, kv.clientop)

	term, isLeader := kv.rf.GetState()

	var opReply OpReply
	switch op.Opcode {
	case OP_CONFIG:
		opReply = kv.processConfigOp(op, term, index, isLeader)
	case OP_MIGRATE:
		opReply = kv.processMigrateOp(op, term, index, isLeader)
	default:
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
			kv.resotreSnapshot(applyMsg.Snapshot, applyMsg.SnapshotIndex)
			kv.lastRaftStateSize = kv.rf.GetRaftStateSize()
		} else {
			// Ignore other types of ApplyMsg.
			raft.LogPrint(raft.WARN, dKvServer, "S%d recv unknown type apply msg %v", kv.me, applyMsg)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) resotreSnapshot(snapshot []byte, index int) {
	if snapshot == nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot is nil", kv.logPrefix)
		return
	}

	byteBuffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(byteBuffer)
	var lastIncludedIndex int
	var database map[string]string
	var clientop map[int64]*OpCache
	var config Cfg
	var dbstat DbStat
	if decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&database) != nil ||
		decoder.Decode(&clientop) != nil ||
		decoder.Decode(&config) != nil ||
		decoder.Decode(&dbstat) != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot Decode() error", kv.logPrefix)
		return
	}
	if index != -1 && index != lastIncludedIndex {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot doesn't match m.SnapshotIndex", kv.logPrefix)
		return
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre clientop %+v", kv.logPrefix, clientop)

	for clientid, op := range clientop {
		_, ok := kv.clientop[clientid]
		if !ok {
			kv.clientop[clientid] = op
		} else {
			if kv.clientop[clientid].SeqId <= op.SeqId {
				kv.clientop[clientid] = op
			}
		}
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre kv clientop %+v", kv.logPrefix, kv.clientop)

	// apply message may apply one more index when create snapshot, one index apply two times
	// 1. current seqid => op was not executed in snapshot
	// 2. current seqid => op has executed one time in snapshot
	// 3. two snapshot come together, snap 1 LLI = 212, not contain 213; snap 2 LLI = 213 op has executed
	for lastApplied := lastIncludedIndex + 1; lastApplied <= kv.lastIncludedIndex; lastApplied++ {
		for clientid, op := range kv.clientop {
			opsnap, ok := clientop[clientid]
			if !ok {
				op.Err = Empty
			} else {
				if op.Index == lastApplied {
					if (op.SeqId > opsnap.SeqId && op.Err != Empty) ||
						(op.SeqId == opsnap.SeqId && op.Err != Empty && opsnap.Err == Empty) {
						op.Err = Empty
					}
				}
			}
		}
	}

	kv.lastIncludedIndex = lastIncludedIndex
	kv.config = config
	kv.dbstat = dbstat.Copy()
	kv.database = database
	/*for key, value := range database {
		// may receive migrate data, so can not use assignment directly
		kv.database[key] = value
	}*/

	raft.LogPrint(raft.INFO, dKvServer, "%s ingest kv config %+v", kv.logPrefix, kv.config)
	raft.LogPrint(raft.INFO, dKvServer, "%s ingest kv dbstat %+v", kv.logPrefix, kv.dbstat)
	raft.LogPrint(raft.INFO, dKvServer, "%s ingest kv database = %+v", kv.logPrefix, kv.database)

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre snapshot LII=%d", kv.logPrefix, kv.lastIncludedIndex)
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
			encoder.Encode(kv.dbstat.Copy())
			kv.rf.Snapshot(kv.lastIncludedIndex, byteBuffer.Bytes())
		}
	}
}

func (kv *ShardKV) newDbStat() DbStat {
	return DbStat{
		Stat:         ACTIVING,
		LastKey:      KEY_MAX,
		LastValue:    "",
		SrcGidShards: make(map[int][]int),
		DstGidShards: make(map[int][]int),
		Config:       Cfg{}}
}

func (kv *ShardKV) processInternalReq(op Op) {

	raft.LogPrint(raft.INFO, dKvServer, "%s internal op = %+v", kv.logPrefix, op)
	index, term, _ := kv.rf.Start(op)

	opCache := &OpCache{term, index, op.Opcode, kv.config.Num, op.SeqId, Empty}
	kv.clientop[op.ClientId] = opCache

	replyCh := make(chan OpReply)
	kv.replyChs[index] = replyCh

	go func() {
		<-replyCh
	}()
}
