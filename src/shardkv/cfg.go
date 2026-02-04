package shardkv

import (
	"time"

	"6.824/raft"
	"6.824/shardctrler"
)

type Cfg shardctrler.Config

func (kv *ShardKV) processConfigOp(op Op, term int, index int, isleader bool) OpReply {

	var opReply OpReply

	cfg := op.Type.(Cfg)

	newcfg := Cfg{}
	newcfg.Num = cfg.Num
	newcfg.Groups = make(map[int][]string)
	newcfg.Shards = cfg.Shards
	for gid, servers := range cfg.Groups {
		newcfg.Groups[gid] = servers
	}

	/*if !isleader { // lastMigrate: no leader when restart
		kv.config = newcfg
		opReply = OpReply{OK, ""}
		return opReply
	}*/

	currNum := kv.config.Num
	if currNum > 1 && currNum == newcfg.Num && !kv.migrateStat[currNum] && kv.migrateStat[currNum-1] {
		kv.config = kv.lastConfig
	}

	//if kv.config.Num+1 == newcfg.Num {
	clientid := op.ClientId
	seqid := op.SeqId

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process config op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if (seqid >= opCache.SeqId && index >= opCache.Index) || !opCache.IsExec {
			opCache = &OpCache{term, index, op.Opcode, seqid, true}
			kv.clientop[clientid] = opCache
			if isleader {
				if kv.config.Num > 0 {
					mode, gidShards := kv.prepareMigration(newcfg)
					for {
						success := kv.dataMigration(newcfg, mode, gidShards)
						if success {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
				} else {
					kv.migrateStat[newcfg.Num] = true
					kv.state = SERVING
					migrate := Migrate{newcfg.Num, true, KeyVal{"MAX", ""}, 0}
					kv.processInternalReq(Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(newcfg.Num), migrate})
				}
			}
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, seqid, true}
		kv.clientop[clientid] = opCache
		if isleader {
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
				kv.migrateStat[newcfg.Num] = true
				kv.state = SERVING
				migrate := Migrate{newcfg.Num, true, KeyVal{"MAX", ""}, 0}
				kv.processInternalReq(Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(newcfg.Num), migrate})
			}
		}
	}
	opReply = OpReply{OK, ""}
	kv.lastConfig = kv.config
	kv.config = newcfg

	//} else {
	//	opReply = OpReply{ErrWrongMigrate, ""}
	//}

	return opReply
}

func (kv *ShardKV) prepareMigration(config Cfg) (int, map[int][]int) {
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
		kv.state = WAITING
		kv.waitPushCount[config.Num] = len(gidShardsSrcMap)
		raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate wait for push state = %d src group count = %d",
			kv.logPrefix, kv.state, kv.waitPushCount[config.Num])
		kv.migrateStat[config.Num] = false
		migrate := Migrate{config.Num, false, KeyVal{"MIN", ""}, kv.waitPushCount[config.Num]}
		kv.processInternalReq(Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(config.Num), migrate})
	} else if len(gidShardsDstMap) > 0 {
		kv.state = MIGRATING
		kv.migrateStat[config.Num] = false
	} else {
		if kv.waitPushCount[config.Num] == 0 {
			kv.state = SERVING
			kv.migrateStat[config.Num] = true
			migrate := Migrate{config.Num, true, KeyVal{"MAX", ""}, kv.waitPushCount[config.Num]}
			kv.processInternalReq(Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(config.Num), migrate})
		}
	}

	if kv.state == MIGRATING {
		return MODE_PUSH, gidShardsDstMap
	} else {
		return MODE_UNKNOWN, nil
	}
}

func (kv *ShardKV) configer() {
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
				num := kv.config.Num
				if kv.migrateStat[kv.config.Num] {
					num = num + 1
				}

				newcfg := kv.cfgck.Query(num)

				kv.state = CONFIGING
				kv.migrateStat[num] = false

				op := Op{OP_CONFIG, CONFIG_CLIENT_ID, int64(num), Cfg(newcfg)}
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
				kv.config = Cfg(config)
				raft.LogPrint(raft.INFO, dKvServer, "%s ticker config = %+v", kv.logPrefix, kv.config)
			}
		}

		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}
