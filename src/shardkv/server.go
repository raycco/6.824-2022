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

const OpProcessTimeOut = 800 * time.Millisecond

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
	shardDbs map[int]*ShardDb

	cfgck   *shardctrler.Clerk
	currCfg Cfg
	lastCfg Cfg

	clientOp map[int64]*OpCache   // client id -> last op
	replyChs map[int]chan OpReply // index -> reply channel

	lastIncludedIndex int
	lastRaftStateSize int

	migrateTasks map[int]MigrateTask

	gidLeaderId map[int]int

	state int

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
	kv.cfgck.Kill()
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
	labgob.Register(ShardDb{})
	labgob.Register(KeyVal{})
	labgob.Register(Migrate{})

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

	kv.shardDbs = make(map[int]*ShardDb)
	kv.clientOp = make(map[int64]*OpCache)
	kv.replyChs = make(map[int]chan OpReply)

	kv.lastIncludedIndex = 0
	kv.lastRaftStateSize = kv.rf.GetRaftStateSize()

	kv.migrateTasks = make(map[int]MigrateTask)
	kv.gidLeaderId = make(map[int]int)

	kv.state = ACTIVING

	go kv.applier()

	go kv.configer()

	go kv.migrater()

	return kv
}

func (kv *ShardKV) isNeedWaitForMigrate(shard int, clientid int64, key string) bool {

	isNeedWait := false
	if kv.currCfg.Num > 1 {
		if kv.lastCfg.Num != kv.currCfg.Num-1 {
			kv.lastCfg = Cfg(kv.cfgck.Query(kv.currCfg.Num - 1))
		}
		gid := kv.lastCfg.Shards[shard]
		raft.LogPrint(raft.INFO, dKvServer, "%s wait for migrate shard %v, gid %v, C%d, key %v, last config %+v",
			kv.logPrefix, shard, gid, clientid, key, kv.lastCfg.Shards)
		db, ok := kv.shardDbs[shard]
		if ok {
			raft.LogPrint(raft.INFO, dKvServer, "%s wait for migrate shard db %+v", kv.logPrefix, db)
		}
		if gid != kv.gid {
			if !ok || (ok && db.GetLastKey() < key) {
				isNeedWait = true
			}
		}
	}

	if clientid <= MIGRATE_CLIENT_ID || !isNeedWait {
		return false
	}
	return true

}

func (kv *ShardKV) isWrongGroup(num int, gid int) bool {

	if num != kv.currCfg.Num {
		return true
	}

	if gid != kv.gid {
		return true
	}

	return false
}

func (kv *ShardKV) processRequest(op Op) OpReply {

	raft.LogPrint(raft.INFO, dKvServer, "%s process request config num = %d, op %+v",
		kv.logPrefix, kv.currCfg.Num, op)

	key := op.Type.(KeyVal).Key

	shard := key2shard(key)
	gid := kv.currCfg.Shards[shard]
	if kv.isWrongGroup(op.Num, gid) {
		return OpReply{ErrWrongGroup, ""}
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return OpReply{ErrWrongLeader, ""}
	}

	if kv.isNeedWaitForMigrate(shard, op.ClientId, key) {
		return OpReply{ErrWaitMigrate, ""}
	}

	if op.Opcode != OP_GET {
		opCache, ok := kv.clientOp[op.ClientId]
		if ok && (op.SeqId < opCache.SeqId ||
			(op.SeqId == opCache.SeqId && opCache.Err == OK)) {
			return OpReply{OK, ""}
		}
	}

	opReply := kv.startOp(op)
	raft.LogPrint(raft.INFO, dKvServer, "%s process request op %+v reply %+v",
		kv.logPrefix, op, opReply)
	return opReply
}

func (kv *ShardKV) opExecute(op Op) OpReply {
	var opReply OpReply

	key := op.Type.(KeyVal).Key
	value := op.Type.(KeyVal).Value

	shard := key2shard(key)
	gid := kv.currCfg.Shards[shard]
	if kv.gid != gid {
		return OpReply{ErrWrongGroup, ""}
	}

	opReply.Err = OK
	db := kv.shardDbs[shard]
	switch op.Opcode {
	case OP_GET:
		_, ok := db.Get(key)
		if !ok {
			opReply.Err = ErrNoKey
			opReply.Value = ""
		} else {
			opReply.Value, _ = db.Get(key)
		}

	case OP_PUT:
		db.Set(key, value)
		opReply.Value, _ = db.Get(key)

	case OP_APPEND:
		db.Append(key, value)
		opReply.Value, _ = db.Get(key)

	case OP_MIGRATE:
		db.Set(key, value)
		db.SetLastKey(key)
		opReply.Value, _ = db.Get(key)

	default:
		opReply.Err = ErrOpNotSupport
	}

	return opReply
}

func (kv *ShardKV) processClientOp(op Op, index int) OpReply {

	var opReply OpReply
	cliId := op.ClientId
	seqId := op.SeqId

	opCache, ok := kv.clientOp[cliId]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process op cache %v",
		kv.logPrefix, index, kv.currCfg.Num, opCache)
	if ok {
		if op.Opcode == OP_GET {
			opReply = kv.opExecute(op)
			if seqId >= opCache.SeqId {
				kv.clientOp[cliId] = &OpCache{index, op.Opcode, kv.currCfg.Num, seqId, Empty}
			}
		} else {
			// 此处有一个问题待解决
			// index 43 apply完执行产生snapshot，但是index 44已经apply并回复给client，
			// client发送下一个append请求，将op提交给raft，index为45，此时op cache中
			// 为index 45，然后restore snapshot回退index到43，重新apply index 44，
			// 虽然index 44 < op cache index 45，但是op cache err为空，则会执行index 44，
			// 导致index 44的数据重复执行
			if (seqId > opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
				opReply = kv.opExecute(op)
				opCache = &OpCache{index, op.Opcode, kv.currCfg.Num, seqId, opReply.Err}
				if opCache.Err != OK {
					// 若第一次执行是ErrWrongGroup，第二次执行时非leader的opCache.Err为ErrWrongGroup，导致数据不一致
					opCache.Err = Empty
				}
				kv.clientOp[cliId] = opCache
			} else {
				opReply = OpReply{OK, ""}
			}
		}
	} else {
		opReply = kv.opExecute(op)
		if op.Opcode == OP_GET {
			opCache = &OpCache{index, op.Opcode, kv.currCfg.Num, seqId, Empty}
		} else {
			opCache = &OpCache{index, op.Opcode, kv.currCfg.Num, seqId, opReply.Err}
			if opCache.Err != OK {
				opCache.Err = Empty
			}
		}
		kv.clientOp[cliId] = opCache
	}

	return opReply
}

func (kv *ShardKV) processInternalOp(op Op, index int, process func() OpReply) OpReply {
	cliId := op.ClientId
	seqId := op.SeqId

	var opReply OpReply

	opCache, ok := kv.clientOp[cliId]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process internal op cache %+v",
		kv.logPrefix, index, kv.currCfg.Num, opCache)
	if ok {
		if (seqId >= opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
			// 版本更新：index 59和index 60先后执行两次，apply index 59时，cache的index为60
			// 虽然index 59 < index 60，但op cache err为空，会更新cache index为59
			kv.clientOp[cliId] = &OpCache{index, op.Opcode, kv.currCfg.Num, seqId, OK}
			opReply = process()
		}
	} else {
		kv.clientOp[cliId] = &OpCache{index, op.Opcode, kv.currCfg.Num, seqId, OK}
		opReply = process()
	}

	return opReply
}

func (kv *ShardKV) processOp(op Op, index int) {
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process op %+v clientOp %v",
		kv.logPrefix, index, kv.currCfg.Num, op, kv.clientOp)

	_, isLeader := kv.rf.GetState()

	var opReply OpReply
	switch op.Opcode {
	case OP_CONFIG:
		opReply = kv.processConfigOp(op, index)
	case OP_MIGRATE:
		opReply = kv.processMigrateOp(op, index)
	case OP_DELETE:
		opReply = kv.processDeleteOp(op, index)
	case OP_NONE:
		opReply = kv.processNoOp(op, index)
	default:
		opReply = kv.processClientOp(op, index)
	}

	if isLeader {
		opCache, cacheok := kv.clientOp[op.ClientId]
		replyCh, chok := kv.replyChs[index]
		if cacheok && chok && opCache.Index == index && replyCh != nil {
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

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre snapshot size = %d", kv.logPrefix, len(snapshot))

	byteBuffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(byteBuffer)
	var lastIncludedIndex int
	var shardDbs map[int]*ShardDb
	var clientOp map[int64]*OpCache
	var currCfg Cfg
	err := decoder.Decode(&lastIncludedIndex)
	if err != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot decode LII error: %+v", kv.logPrefix, err)
		return
	}

	err = decoder.Decode(&shardDbs)
	if err != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot decode shard dbs error: %+v", kv.logPrefix, err)
		return
	}

	err = decoder.Decode(&clientOp)
	if err != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot decode client seqs error: %+v", kv.logPrefix, err)
		return
	}

	err = decoder.Decode(&currCfg)
	if err != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot decode config error: %+v", kv.logPrefix, err)
		return
	}

	/*if decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&shardDbs) != nil ||
		decoder.Decode(&clientOp) != nil ||
		decoder.Decode(&config) != nil {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot Decode() error", kv.logPrefix)
		return
	}*/

	if index != -1 && index != lastIncludedIndex {
		raft.LogPrint(raft.ERROR, dKvServer, "%s snapshot doesn't match m.SnapshotIndex", kv.logPrefix)
		return
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre clientOp %+v", kv.logPrefix, clientOp)

	for cliId, opSnap := range clientOp {
		op, ok := kv.clientOp[cliId]
		if !ok || op.SeqId <= opSnap.SeqId {
			kv.clientOp[cliId] = opSnap
		}
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre kv clientOp %+v", kv.logPrefix, kv.clientOp)

	// apply message may apply one more index when create snapshot, one index apply two times
	// 1. current seqid => op was not executed in snapshot
	// 2. current seqid => op has executed one time in snapshot
	// 3. two snapshot come together, snap 1 LLI = 212, not contain 213; snap 2 LLI = 213 op has executed
	for lastApplied := lastIncludedIndex + 1; lastApplied <= kv.lastIncludedIndex; lastApplied++ {
		for cliId, op := range kv.clientOp {
			opSnap, ok := clientOp[cliId]
			if !ok {
				op.Err = Empty
			} else {
				if op.Index == lastApplied {
					if (op.SeqId > opSnap.SeqId && op.Err != Empty) ||
						(op.SeqId == opSnap.SeqId && op.Err != Empty && opSnap.Err == Empty) {
						op.Err = Empty
					}
				}
			}
		}
	}

	kv.lastIncludedIndex = lastIncludedIndex
	kv.currCfg = currCfg
	kv.shardDbs = shardDbs

	raft.LogPrint(raft.INFO, dKvServer, "%s ingest kv config %+v", kv.logPrefix, kv.currCfg)
	for shard, sharddb := range kv.shardDbs {
		raft.LogPrint(raft.INFO, dKvServer, "%s ingest kvdb shard %d db %+v", kv.logPrefix, shard, sharddb)
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s resotre snapshot LII=%d", kv.logPrefix, kv.lastIncludedIndex)
}

func (kv *ShardKV) createSnapshot(force bool) {
	if kv.maxraftstate != -1 {
		raftStateSize := kv.rf.GetRaftStateSize()
		if raftStateSize > kv.maxraftstate || force {
			raft.LogPrint(raft.INFO, dKvServer, "%s RSS=%d LRSS=%d LII=%d",
				kv.logPrefix, raftStateSize, kv.lastRaftStateSize, kv.lastIncludedIndex)
			raft.LogPrint(raft.INFO, dKvServer, "%s database = %+v", kv.logPrefix, kv.shardDbs)
			raft.LogPrint(raft.INFO, dKvServer, "%s clientOp = %+v", kv.logPrefix, kv.clientOp)

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			encoder.Encode(kv.lastIncludedIndex)
			encoder.Encode(kv.shardDbs)
			encoder.Encode(kv.clientOp)
			encoder.Encode(kv.currCfg)
			kv.rf.Snapshot(kv.lastIncludedIndex, byteBuffer.Bytes())
		}
	}
}

func (kv *ShardKV) startOp(op Op) OpReply {

	raft.LogPrint(raft.INFO, dKvServer, "%s start op %+v", kv.logPrefix, op)
	var opReply OpReply
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		opReply = OpReply{ErrWrongLeader, ""}
	} else {
		opCache := &OpCache{index, op.Opcode, kv.currCfg.Num, op.SeqId, Empty}
		kv.clientOp[op.ClientId] = opCache

		// 无缓冲 channel：发送和接收必须同步进行，这里会解锁后select，若在没有进入select
		// 就往channel写数据容易阻塞applier协程，因此改为带缓冲的channel。
		replyCh := make(chan OpReply, 1)
		kv.replyChs[index] = replyCh

		kv.mu.Unlock()
		// 版本更新：index 5已经timeout，目前replyCh是index 6，apply却是index 5，
		// 此时向index 5写入数据阻塞，在applier中需要判断index是否匹配
		select {
		case opReply = <-replyCh:
		case <-time.After(OpProcessTimeOut):
			opReply.Err = ErrOpTimeOut
		}
		kv.mu.Lock()
	}
	raft.LogPrint(raft.INFO, dKvServer, "%s end op index %d reply %+v", kv.logPrefix, index, opReply)
	return opReply
}
