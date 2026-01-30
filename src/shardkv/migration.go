package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	PUSH_CLIENT_ID = -1
	PULL_CLIENT_ID = -2
	CFG_CLIENT_ID  = -3
)

const (
	MODE_UNKNOWN = iota
	MODE_PUSH
	MODE_PULL
)

const (
	INIT = iota
	SERVING
	WAIT
	MIGRATING
	MIGRATED
)

type RequestMigrateArgs struct {
	Num    int
	Gid    int
	Config shardctrler.Config
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

func (kv *ShardKV) processMigrateOp(op Op, term int, index int, isleader bool) OpReply {

	cfg := op.Cfg.(shardctrler.Config)
	var opReply OpReply
	newcfg := shardctrler.Config{}
	newcfg.Num = cfg.Num
	newcfg.Groups = make(map[int][]string)
	newcfg.Shards = cfg.Shards
	for gid, servers := range cfg.Groups {
		newcfg.Groups[gid] = servers
	}

	if !isleader { // todo: no leader when restart
		kv.config = newcfg
		opReply = OpReply{OK, ""}
		return opReply
	}

	//if kv.config.Num+1 == newcfg.Num {
	clientid := op.ClientId
	seqid := op.SeqId

	opCache, ok := kv.clientop[clientid]
	if ok {
		if seqid >= opCache.SeqId && index >= opCache.Index || !opCache.IsExec {
			opCache = &OpCache{term, index, op.Opcode, seqid, true}
			kv.clientop[clientid] = opCache
			if kv.config.Num > 0 {
				//kv.dataMigration(newcfg, true)
				mode, gidShards := kv.prepareMigration(newcfg)
				for {
					success := kv.dataMigration(newcfg, mode, gidShards)
					if success {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			} else {
				kv.state = SERVING
			}
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, seqid, true}
		kv.clientop[clientid] = opCache
		if kv.config.Num > 0 {
			//kv.dataMigration(newcfg, true)
			mode, gidShards := kv.prepareMigration(newcfg)
			for {
				success := kv.dataMigration(newcfg, mode, gidShards)
				if success {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			kv.state = SERVING
		}
	}
	opReply = OpReply{OK, ""}
	kv.config = newcfg
	//} else {
	//	opReply = OpReply{ErrWrongMigrate, ""}
	//}

	return opReply
}

func (kv *ShardKV) prepareMigration(config shardctrler.Config) (int, map[int][]int) {
	raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate %v => %v", kv.logPrefix, kv.config.Shards, config.Shards)

	gidShardsSrcMap := make(map[int][]int)
	gidShardsDstMap := make(map[int][]int)
	for shard := 0; shard < len(config.Shards); shard++ {
		ngid := config.Shards[shard]
		ogid := kv.config.Shards[shard]
		if ogid != ngid {
			if ogid == kv.gid { // push
				// start migrating the data for that shard to the replica group that is taking over ownership
				gidShardsDstMap[ngid] = append(gidShardsDstMap[ngid], shard)
			} else if ngid == kv.gid { // pull
				// wait for the previous owner to send over the old shard data
				gidShardsSrcMap[ogid] = append(gidShardsSrcMap[ogid], shard)
			} else {
				// other group need wait
			}
		}
	}

	/*if len(gidShardsSrcMap) > 0 {
		return MODE_PULL, gidShardsSrcMap
	} else if len(gidShardsDstMap) > 0 {
		return MODE_PUSH, gidShardsDstMap
	} else {
		return MODE_UNKNOWN, nil
	}*/
	if len(gidShardsSrcMap) > 0 {
		kv.state = WAIT
		kv.waitPushCount[config.Num] = len(gidShardsSrcMap)
		raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate wait for push state = %d src group count = %d",
			kv.logPrefix, kv.state, kv.waitPushCount[config.Num])
	} else if len(gidShardsDstMap) > 0 {
		kv.state = MIGRATING
	} else {
		kv.state = SERVING
	}

	if kv.state == MIGRATING {
		return MODE_PUSH, gidShardsDstMap
	} else {
		return MODE_UNKNOWN, nil
	}
}

func (kv *ShardKV) dataMigration(config shardctrler.Config, mode int, gidShards map[int][]int) bool {
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
	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request", kv.logPrefix, isLeader)
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
	kv.state = MIGRATING

	byteBuffer := bytes.NewBuffer(args.Data)
	decoder := labgob.NewDecoder(byteBuffer)

	sharddb := make(map[string]string)

	decoder.Decode(&sharddb)

	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request args %v", kv.logPrefix, isLeader, sharddb)

	for key, value := range sharddb {
		kv.migratingDb[key] = value
	}

	kv.migratingGidConfig[args.Gid] = args.Config
	//kv.createSnapshot(true)

	go kv.migrater(sharddb)
	//kv.cfgUpdateCond.Wait()
	reply.Err = OK
}

func (kv *ShardKV) MigrateProcess(args *RequestMigrateProgressArgs, reply *RequestMigrateProgressReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.state = SERVING
	_, isLeader := kv.rf.GetState()
	if isLeader {
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

	kv.state = MIGRATING

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
	defer kv.mu.Unlock()

	kv.state = MIGRATING

	for key, value := range sharddb {
		kv.lastPushSeqId = kv.lastPushSeqId + 1
		op := Op{OP_PUT, PUSH_CLIENT_ID, kv.lastPushSeqId, key, value, nil}
		kv.processRequest(op)
		kv.migratingCond.Signal()
		delete(kv.migratingDb, key)
	}

	if len(kv.migratingDb) <= 0 && kv.waitPushCount[kv.config.Num] == len(kv.migratingGidConfig) {
		kv.state = SERVING
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
}
