package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
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

type Migrate struct {
	Num       int
	Complete  bool
	KeyVal    KeyVal
	WaitCount int
}

type DbStat struct {
	Stat         int
	LastKey      string
	LastValue    string
	SrcGidShards map[int][]int
	DstGidShards map[int][]int
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
		if (seqid >= opCache.SeqId && index >= opCache.Index) || !opCache.IsExec {
			opCache = &OpCache{term, index, op.Opcode, seqid, true}
			kv.clientop[clientid] = opCache
			switch dbstat.LastKey {
			case "MAX":
				kv.migratingCond.Signal()
				kv.cfgUpdateCond.Signal()
			case "MIN":
				if isleader && dbstat.Stat == PUSHING {
					for {
						success := kv.dataMigration(dbstat.Config, MODE_PUSH, dbstat.DstGidShards)
						if success {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
				}
			default:
				kv.database[dbstat.LastKey] = dbstat.LastValue
				opReply.Value = kv.database[dbstat.LastKey]
			}
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, seqid, true}
		kv.clientop[clientid] = opCache
		switch dbstat.LastKey {
		case "MAX":
			kv.migratingCond.Signal()
			kv.cfgUpdateCond.Signal()
		case "MIN":
			if isleader && dbstat.Stat == PUSHING {
				for {
					success := kv.dataMigration(dbstat.Config, MODE_PUSH, dbstat.DstGidShards)
					if success {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		default:
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

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			encoder.Encode(sharddb)
			args.Data = byteBuffer.Bytes()
			args.Num = config.Num
			args.Gid = kv.gid
			args.Config = kv.config
			if servers, ok := config.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {
					var reply RequestMigrateReply
					srv := kv.make_end(servers[si])
					raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d args %v", kv.logPrefix, shards, gid, si, sharddb)

					ok := srv.Call("ShardKV.MigratePush", &args, &reply)
					if ok {
						if reply.Err == OK {
							raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d success", kv.logPrefix, shards, gid, si)
							delete(gidShards, gid)
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

							decoder.Decode(&sharddb)
							go kv.migrater(sharddb)
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

	for {
		if args.Num == kv.config.Num {
			break
		}
		kv.migrateCond.Wait()
	}

	byteBuffer := bytes.NewBuffer(args.Data)
	decoder := labgob.NewDecoder(byteBuffer)

	sharddb := make(map[string]string)

	decoder.Decode(&sharddb)

	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request args %v", kv.logPrefix, isLeader, sharddb)

	for key, value := range sharddb {
		kv.migratingDb[key] = value
	}

	kv.migratingGidConfig[args.Gid] = args.Config

	go kv.migrater(sharddb)
	//kv.cfgUpdateCond.Wait()
	reply.Err = OK
}

func (kv *ShardKV) MigrateProcess(args *RequestMigrateProgressArgs, reply *RequestMigrateProgressReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if isLeader {
		kv.convertToServing()
		op := Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
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

func (kv *ShardKV) migrater(sharddb map[string]string) {

	kv.mu.Lock()

	kv.dbstat.Stat = MIGRATING

	for key, value := range sharddb {
		kv.dbstat.LastKey = key
		kv.dbstat.LastValue = value
		op := Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat}
		raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
		index, term, _ := kv.rf.Start(op)

		opCache := &OpCache{term, index, op.Opcode, op.SeqId, false}
		kv.clientop[op.ClientId] = opCache

		replyCh := make(chan OpReply)
		kv.replyChs[index] = replyCh

		kv.mu.Unlock()
		opReply := <-replyCh
		kv.mu.Lock()
		raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, opReply)

		kv.migratingCond.Signal()
		delete(kv.migratingDb, key)
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s waitPushCount = %d, config count = %d",
		kv.logPrefix, len(kv.dbstat.SrcGidShards), len(kv.migratingGidConfig))

	if len(kv.migratingDb) <= 0 && len(kv.dbstat.SrcGidShards) == len(kv.migratingGidConfig) {
		kv.convertToServing()
		op := Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(kv.config.Num), kv.dbstat.Copy()}
		kv.processInternalReq(op)
		kv.cfgUpdateCond.Signal()

		for gid, config := range kv.migratingGidConfig {
			if servers, ok := config.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {
					var reply RequestMigrateProgressReply
					var args RequestMigrateProgressArgs
					args.IsComplete = true
					srv := kv.make_end(servers[si])
					ok := srv.Call("ShardKV.MigrateProcess", &args, &reply)
					raft.LogPrint(raft.INFO, dKvServer, "%s migrate complete => G%d-S%d", kv.logPrefix, gid, si)
					if ok {
						if reply.Err == OK {
							break
						}
					}
				}
			}
			delete(kv.migratingGidConfig, gid)
		}
	}
	kv.mu.Unlock()
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
