package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	MIGRATE_CLIENT_ID = -1
	CONFIG_CLIENT_ID  = -2
)

const (
	KEY_MAX = "MAX"
	KEY_MIN = "MIN"
)

const (
	MODE_UNKNOWN = iota
	MODE_PUSH
	MODE_PULL
)

type RequestMigrateArgs struct {
	Num    int
	Gid    int
	Config Cfg
	Data   []byte // raw bytes of the shard to migrate
}

type RequestMigrateReply struct {
	RequestMigrateArgs
	Err Err
}

type RequestMigrateProgressArgs struct {
	IsComplete bool
}

type RequestMigrateProgressReply struct {
	Err Err
}

type MigrateTask struct {
	sharddb map[string]string
	cliseq  map[int64]int64
}

func (src *MigrateTask) Copy() MigrateTask {
	var task MigrateTask
	task.sharddb = make(map[string]string)
	for key, value := range src.sharddb {
		task.sharddb[key] = value
	}

	task.cliseq = make(map[int64]int64)
	for clientid, seqid := range src.cliseq {
		task.cliseq[clientid] = seqid
	}
	return task
}

type DbStat struct {
	Stat         int
	LastKey      string
	LastValue    string
	SrcGidShards map[int][]int
	DstGidShards map[int][]int
	ClientSeq    map[int64]int64
	Config       Cfg
}

func (src *DbStat) Copy() DbStat {
	var dst DbStat
	dst.Stat = src.Stat
	dst.LastKey = src.LastKey
	dst.LastValue = src.LastValue

	dst.SrcGidShards = make(map[int][]int)
	for gid, shards := range src.SrcGidShards {
		dst.SrcGidShards[gid] = shards
	}

	dst.DstGidShards = make(map[int][]int)
	for gid, shards := range src.DstGidShards {
		dst.DstGidShards[gid] = shards
	}

	dst.ClientSeq = make(map[int64]int64)
	for clientid, seqid := range src.ClientSeq {
		dst.ClientSeq[clientid] = seqid
	}
	dst.Config = src.Config.Copy()
	return dst
}

func (kv *ShardKV) processMigrateOp(op Op, term int, index int, isleader bool) OpReply {
	clientid := op.ClientId
	seqid := op.SeqId

	dbstat := op.Type.(DbStat)
	kv.dbstat = dbstat.Copy()

	var opReply OpReply

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if (seqid >= opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
			kv.clientop[clientid] = opCache
			switch dbstat.LastKey {
			case "MAX":
				kv.migratingCond.Signal()
				kv.cfgUpdateCond.Signal()
			case "MIN":
				if isleader && dbstat.Stat == PUSHING {
					gidShards := dbstat.Copy().DstGidShards
					for {
						success := kv.dataMigration(dbstat.Config, MODE_PUSH, gidShards)
						if success {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
				}
			default:
				for clientid, seqid := range dbstat.ClientSeq {
					op, ok := kv.clientop[clientid]
					opCache := OpCache{term, index, OP_PUT, kv.config.Num, seqid, OK}
					if !ok || op.SeqId < seqid {
						kv.clientop[clientid] = &opCache
					}
				}
				kv.database[dbstat.LastKey] = dbstat.LastValue
				opReply.Value = kv.database[dbstat.LastKey]
			}
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
		kv.clientop[clientid] = opCache
		switch dbstat.LastKey {
		case "MAX":
			kv.migratingCond.Signal()
			kv.cfgUpdateCond.Signal()
		case "MIN":
			if isleader && dbstat.Stat == PUSHING {
				gidShards := dbstat.Copy().DstGidShards
				for {
					success := kv.dataMigration(dbstat.Config, MODE_PUSH, gidShards)
					if success {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		default:
			for clientid, seqid := range dbstat.ClientSeq {
				op, ok := kv.clientop[clientid]
				opCache := OpCache{term, index, OP_PUT, kv.config.Num, seqid, OK}
				if !ok || op.SeqId < seqid {
					kv.clientop[clientid] = &opCache
				}
			}

			kv.database[dbstat.LastKey] = dbstat.LastValue
			opReply.Value = kv.database[dbstat.LastKey]
		}
	}

	if dbstat.LastKey == "MIN" || dbstat.LastKey == "MAX" {
		kv.lastConfig = kv.config.Copy()
		kv.config = kv.dbstat.Config.Copy()
		kv.migrateCond.Broadcast()
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d state %v",
		kv.logPrefix, index, kv.config.Num, kv.dbstat)
	return opReply
}

type PushReply struct {
	ok  bool
	Err Err
}

func (kv *ShardKV) dataMigration(config Cfg, mode int, gidShards map[int][]int) bool {
	/*if kv.state == MIGRATING {
		return
	}*/
	switch mode {
	case MODE_PUSH:
		for gid, shards := range gidShards {

			var args RequestMigrateArgs
			sharddb := make(map[string]string)
			for _, shard := range shards {
				for key, value := range kv.database {
					if shard == key2shard(key) {
						sharddb[key] = value
					}
				}
			}

			cliseq := make(map[int64]int64)
			for clientid, op := range kv.clientop {
				if clientid >= 0 && op.Err == OK {
					cliseq[clientid] = op.SeqId
				}
			}

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			encoder.Encode(sharddb)
			encoder.Encode(cliseq)
			args.Data = byteBuffer.Bytes()
			args.Num = config.Num
			args.Gid = kv.gid
			if kv.config.Num+1 == config.Num {
				args.Config = kv.config
			} else {
				args.Config = kv.lastConfig
			}
			if servers, ok := config.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {

					srv := kv.make_end(servers[si])
					raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d args %v", kv.logPrefix, shards, gid, si, sharddb)
					var reply RequestMigrateReply
					ok := srv.Call("ShardKV.MigratePush", &args, &reply)

					if ok {
						raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d %s", kv.logPrefix, shards, gid, si, reply.Err)
						if reply.Err == OK {
							delete(gidShards, gid)
							break
						} else if reply.Err == ErrCfgExpired || reply.Err == ErrMigrateComplete {
							delete(gidShards, gid)
							kv.convertToServing()
							op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
							kv.processInternalReq(op)
							break
						}
					} else {
						raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d fail", kv.logPrefix, shards, gid, si)
					}
				}
			}
		}
		if len(gidShards) > 0 {
			return false
		} else {
			return true
		}
	case MODE_PULL:
		for gid, shards := range gidShards {

			if servers, ok := config.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])

					var args RequestMigrateArgs
					args.Num = config.Num
					args.Gid = gid
					args.Config = config
					var reply RequestMigrateReply
					raft.LogPrint(raft.INFO, dKvServer, "%s pull data migrate shards %d => G%d-S%d", kv.logPrefix, shards, gid, si)
					ok := srv.Call("ShardKV.MigratePull", &args, &reply)
					if ok {
						if reply.Err == OK {
							raft.LogPrint(raft.INFO, dKvServer, "%s pull data migrate shards %d => G%d-S%d success", kv.logPrefix, shards, gid, si)
							byteBuffer := bytes.NewBuffer(reply.Data)
							decoder := labgob.NewDecoder(byteBuffer)
							sharddb := make(map[string]string)
							cliseq := make(map[int64]int64)

							decoder.Decode(&sharddb)
							decoder.Decode(&cliseq)
							go kv.migrater(sharddb, cliseq, gid)
							delete(gidShards, gid)
							break
						} else {
							raft.LogPrint(raft.INFO, dKvServer, "%s pull data migrate shards %d => G%d-S%d wrong leader", kv.logPrefix, shards, gid, si)
						}
					} else {
						raft.LogPrint(raft.INFO, dKvServer, "%s pull data migrate shards %d => G%d-S%d fail", kv.logPrefix, shards, gid, si)
					}

				}
			}
		}
		if len(gidShards) > 0 {
			return false
		} else {
			return true
		}
	default:
		return true
	}
}

func (kv *ShardKV) MigratePush(args *RequestMigrateArgs, reply *RequestMigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request num = %d, args num = %d",
		kv.logPrefix, isLeader, kv.config.Num, args.Num)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if args.Num < kv.config.Num {
		reply.Err = ErrCfgExpired
		return
	}

	for {
		if args.Num == kv.config.Num {
			break
		}
		kv.migrateCond.Wait()
	}

	_, ok := kv.dbstat.SrcGidShards[args.Gid]
	if !ok || kv.dbstat.Stat == SERVING {
		reply.Err = ErrMigrateComplete
		return
	}

	_, ok1 := kv.migratingGidConfig[args.Gid]
	if kv.dbstat.Stat == MIGRATING && ok1 {
		reply.Err = OK
		return
	}

	byteBuffer := bytes.NewBuffer(args.Data)
	decoder := labgob.NewDecoder(byteBuffer)

	var task MigrateTask
	task.sharddb = make(map[string]string)
	task.cliseq = make(map[int64]int64)

	decoder.Decode(&task.sharddb)
	decoder.Decode(&task.cliseq)

	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request args %v", kv.logPrefix, isLeader, task.sharddb)

	for key, value := range task.sharddb {
		kv.migratingDb[key] = value
	}

	kv.migrateTasks[args.Gid] = task.Copy()

	kv.migratingGidConfig[args.Gid] = args.Config

	//go kv.migrater(task.sharddb, task.cliseq, args.Gid)
	reply.Err = OK
}

func (kv *ShardKV) MigrateProcess(args *RequestMigrateProgressArgs, reply *RequestMigrateProgressReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if isLeader {
		kv.convertToServing()
		op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
		kv.processInternalReq(op)
		raft.LogPrint(raft.INFO, dKvServer, "%s receive migrate complete", kv.logPrefix)
		kv.cfgUpdateCond.Signal()
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) MigratePull(args *RequestMigrateArgs, reply *RequestMigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive pull data request", kv.logPrefix, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		if args.Num == kv.config.Num {
			break
		}
		kv.migrateCond.Wait()
	}

	sharddb := make(map[string]string)
	for _, shard := range args.Config.Shards {
		for key, value := range kv.database {
			if shard == key2shard(key) {
				sharddb[key] = value
			}
		}
	}
	byteBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(byteBuffer)
	encoder.Encode(sharddb)
	reply.Data = byteBuffer.Bytes()
	reply.Err = OK
	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive pull data request args %v", kv.logPrefix, isLeader, sharddb)
}

func (kv *ShardKV) migrater(sharddb map[string]string, cliseq map[int64]int64, srcgid int) {

	kv.mu.Lock()

	kv.dbstat.Stat = MIGRATING

	/*if len(cliseq) > 0 {
		kv.dbstat.LastKey = KEY_MIN // 会导致last config马上换成最新的
		kv.dbstat.ClientSeq = cliseq
		op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat}
		raft.LogPrint(raft.INFO, dKvServer, "%s client seq op = %+v", kv.logPrefix, op)
		index, term, _ := kv.rf.Start(op)
		opCache := &OpCache{term, index, op.Opcode, kv.config.Num, op.SeqId, Empty}
		kv.clientop[op.ClientId] = opCache

		replyCh := make(chan OpReply)
		kv.replyChs[index] = replyCh

		kv.mu.Unlock()
		opReply := <-replyCh
		kv.mu.Lock()
		raft.LogPrint(raft.INFO, dKvServer, "%s finish client seq reply = %+v", kv.logPrefix, opReply)
	}*/

	count := 0
	for key, value := range sharddb {
		kv.dbstat.LastKey = key
		kv.dbstat.LastValue = value
		kv.dbstat.ClientSeq = cliseq
		if count+1 == len(sharddb) {
			delete(kv.dbstat.SrcGidShards, srcgid)
		}
		op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
		raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
		index, term, _ := kv.rf.Start(op)

		opCache := &OpCache{term, index, op.Opcode, kv.config.Num, op.SeqId, Empty}
		kv.clientop[op.ClientId] = opCache

		replyCh := make(chan OpReply)
		kv.replyChs[index] = replyCh

		kv.mu.Unlock()
		opReply := <-replyCh
		kv.mu.Lock()
		raft.LogPrint(raft.INFO, dKvServer, "%s migrate op reply = %+v", kv.logPrefix, opReply)

		kv.migratingCond.Signal()
		delete(kv.migratingDb, key)
		count++
	}

	if len(sharddb) == 0 {
		delete(kv.dbstat.SrcGidShards, srcgid)
	}

	replyChan := make(chan PushReply)

	if servers, ok := kv.migratingGidConfig[srcgid].Groups[srcgid]; ok {
		for si := 0; si < len(servers); si++ {

			var args RequestMigrateProgressArgs
			args.IsComplete = true
			srv := kv.make_end(servers[si])

			go func(srv *labrpc.ClientEnd, args *RequestMigrateProgressArgs) {
				var reply RequestMigrateProgressReply
				ok := srv.Call("ShardKV.MigrateProcess", args, &reply)
				replyChan <- PushReply{ok, reply.Err}
			}(srv, &args)
			kv.mu.Unlock()
			pushReply := <-replyChan
			kv.mu.Lock()

			raft.LogPrint(raft.INFO, dKvServer, "%s migrate complete => G%d-S%d", kv.logPrefix, srcgid, si)
			if pushReply.ok {
				if pushReply.Err == OK {
					break
				}
			}
		}
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s wait push count = %d, config count = %d",
		kv.logPrefix, len(kv.dbstat.SrcGidShards), len(kv.migratingGidConfig))

	if len(kv.migratingDb) <= 0 && len(kv.dbstat.SrcGidShards) <= 0 {
		kv.convertToServing()
		op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
		kv.processInternalReq(op)
		kv.cfgUpdateCond.Signal()

		for gid, _ := range kv.migratingGidConfig {
			delete(kv.migratingGidConfig, gid)
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) migraterex() {

	for !kv.killed() {
		kv.mu.Lock()

		for srcgid, task := range kv.migrateTasks {

			kv.dbstat.Stat = MIGRATING

			count := 0
			for key, value := range task.sharddb {
				kv.dbstat.LastKey = key
				kv.dbstat.LastValue = value
				kv.dbstat.ClientSeq = task.cliseq
				if count+1 == len(task.sharddb) {
					delete(kv.dbstat.SrcGidShards, srcgid)
				}
				op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
				raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
				index, term, _ := kv.rf.Start(op)

				opCache := &OpCache{term, index, op.Opcode, kv.config.Num, op.SeqId, Empty}
				kv.clientop[op.ClientId] = opCache

				replyCh := make(chan OpReply)
				kv.replyChs[index] = replyCh

				kv.mu.Unlock()
				opReply := <-replyCh
				kv.mu.Lock()
				raft.LogPrint(raft.INFO, dKvServer, "%s migrate op reply = %+v", kv.logPrefix, opReply)

				kv.migratingCond.Signal()
				delete(kv.migratingDb, key)
				count++
			}

			if len(task.sharddb) == 0 {
				delete(kv.dbstat.SrcGidShards, srcgid)
			}

			replyChan := make(chan PushReply)

			if servers, ok := kv.migratingGidConfig[srcgid].Groups[srcgid]; ok {
				for si := 0; si < len(servers); si++ {

					var args RequestMigrateProgressArgs
					args.IsComplete = true
					srv := kv.make_end(servers[si])

					go func(srv *labrpc.ClientEnd, args *RequestMigrateProgressArgs) {
						var reply RequestMigrateProgressReply
						ok := srv.Call("ShardKV.MigrateProcess", args, &reply)
						replyChan <- PushReply{ok, reply.Err}
					}(srv, &args)
					kv.mu.Unlock()
					pushReply := <-replyChan
					kv.mu.Lock()

					raft.LogPrint(raft.INFO, dKvServer, "%s migrate complete => G%d-S%d", kv.logPrefix, srcgid, si)
					if pushReply.ok {
						if pushReply.Err == OK {
							break
						}
					}
				}
			}

			raft.LogPrint(raft.INFO, dKvServer, "%s wait push count = %d, config count = %d",
				kv.logPrefix, len(kv.dbstat.SrcGidShards), len(kv.migratingGidConfig))

			if len(kv.migratingDb) <= 0 && len(kv.dbstat.SrcGidShards) <= 0 {
				//此时没来得及转为serving，server killed，applier已经退出了，该如何处理
				kv.convertToServing()
				op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
				kv.processInternalReq(op)
				kv.cfgUpdateCond.Signal()

				for gid, _ := range kv.migratingGidConfig {
					delete(kv.migratingGidConfig, gid)
				}
			}

			delete(kv.migrateTasks, srcgid)
		}
		kv.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) convertToServing() {
	kv.dbstat.Stat = SERVING
	kv.dbstat.LastKey = KEY_MAX
	kv.dbstat.LastValue = ""
	for gid := range kv.dbstat.DstGidShards {
		delete(kv.dbstat.DstGidShards, gid)
	}
	for gid := range kv.dbstat.SrcGidShards {
		delete(kv.dbstat.SrcGidShards, gid)
	}
}
