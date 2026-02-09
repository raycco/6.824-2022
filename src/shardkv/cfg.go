package shardkv

import (
	"time"

	"6.824/raft"
	"6.824/shardctrler"
)

type Cfg shardctrler.Config

func (src *Cfg) Copy() Cfg {
	var dst Cfg
	dst.Num = src.Num
	dst.Groups = make(map[int][]string)
	dst.Shards = src.Shards
	for gid, servers := range src.Groups {
		dst.Groups[gid] = servers
	}
	return dst
}

func (kv *ShardKV) processConfigOp(op Op, term int, index int, isleader bool) OpReply {

	var opReply OpReply

	dbstat := op.Type.(DbStat)

	newcfg := dbstat.Config.Copy()

	/*if !isleader { // lastMigrate: no leader when restart
		kv.config = newcfg
		opReply = OpReply{OK, ""}
		return opReply
	}*/

	/*currNum := kv.config.Num
	if currNum > 1 && currNum == newcfg.Num && kv.dbstat.Stat != SERVING {
		kv.config = kv.lastConfig
	}*/

	if kv.config.Num+1 == newcfg.Num {
		kv.dbstat = dbstat.Copy()

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
						kv.prepareMigration(newcfg)
						/*for {
							success := kv.dataMigration(newcfg, mode, gidShards)
							if success {
								break
							}
							time.Sleep(100 * time.Millisecond)
						}*/
					} else {
						kv.dbstat.Stat = SERVING
						op := Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(newcfg.Num), kv.dbstat.Copy()}
						kv.processInternalReq(op)
					}
				}
			}
		} else {
			opCache = &OpCache{term, index, op.Opcode, seqid, true}
			kv.clientop[clientid] = opCache
			if isleader {
				if kv.config.Num > 0 {
					kv.prepareMigration(newcfg)
					/*for {
						success := kv.dataMigration(newcfg, mode, gidShards)
						if success {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}*/
				} else {
					kv.dbstat.Stat = SERVING
					op := Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(newcfg.Num), kv.dbstat.Copy()}
					kv.processInternalReq(op)
				}
			}
		}
		opReply = OpReply{OK, ""}
	} else {
		opReply = OpReply{ErrDataMigrate, ""}
	}

	return opReply
}

func (kv *ShardKV) prepareMigration(config Cfg) (int, map[int][]int) {
	raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate %v => %v",
		kv.logPrefix, kv.config.Shards, config.Shards)

	//gidShardsSrcMap := make(map[int][]int)
	//gidShardsDstMap := make(map[int][]int)
	for shard := 0; shard < len(config.Shards); shard++ {
		ngid := config.Shards[shard]
		ogid := kv.config.Shards[shard]
		if ogid != ngid {
			if ogid == kv.gid { // push
				// start migrating the data for that shard to the replica group that is taking over ownership
				//gidShardsDstMap[ngid] = append(gidShardsDstMap[ngid], shard)
				kv.dbstat.DstGidShards[ngid] = append(kv.dbstat.DstGidShards[ngid], shard)
			} else if ngid == kv.gid { // pull
				// wait for the previous owner to send over the old shard data
				//gidShardsSrcMap[ogid] = append(gidShardsSrcMap[ogid], shard)
				kv.dbstat.SrcGidShards[ogid] = append(kv.dbstat.SrcGidShards[ogid], shard)
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
	if len(kv.dbstat.SrcGidShards) > 0 {
		kv.dbstat.Stat = WAITING
		kv.dbstat.LastKey = KEY_MIN
		raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate wait for push state = %d src group count = %d",
			kv.logPrefix, kv.dbstat.Stat, len(kv.dbstat.SrcGidShards))
	} else if len(kv.dbstat.DstGidShards) > 0 {
		kv.dbstat.Stat = PUSHING
		kv.dbstat.LastKey = KEY_MIN
	} else {
		kv.dbstat.Stat = SERVING
		kv.dbstat.LastKey = KEY_MAX
	}

	op := Op{OP_MIGRATE, MIGRATE_CLIENT_ID, int64(config.Num), kv.dbstat.Copy()}
	kv.processInternalReq(op)

	if kv.dbstat.Stat == PUSHING {
		return MODE_PUSH, kv.dbstat.DstGidShards
	} else {
		return MODE_UNKNOWN, nil
	}
}

func (kv *ShardKV) configer() {
	for !kv.killed() {

		kv.mu.Lock()

		if kv.config.Num == 0 {
			kv.dbstat.Stat = SERVING
		}

		_, isLeader := kv.rf.GetState()
		if isLeader {

			/*for {
				if len(kv.migratingDb) <= 0 && kv.dbstat.Stat == SERVING {
					break
				}
				kv.cfgUpdateCond.Wait()
			}*/

			if kv.dbstat.Config.Num == kv.config.Num {
				if kv.dbstat.Stat == SERVING {
					var config Cfg
					config = Cfg(kv.cfgck.Query(kv.config.Num + 1))
					if config.Num == kv.config.Num+1 {
						dbstat := kv.newDbStat()
						dbstat.Config = config.Copy()
						dbstat.Stat = CONFIGING

						op := Op{OP_CONFIG, CONFIG_CLIENT_ID, int64(config.Num), dbstat}
						raft.LogPrint(raft.INFO, dKvServer, "%s num = %d ticker op = %+v", kv.logPrefix, kv.config.Num, op)
						index, term, _ := kv.rf.Start(op)

						opCache := &OpCache{term, index, op.Opcode, op.SeqId, false}
						kv.clientop[op.ClientId] = opCache

						/*replyCh := make(chan OpReply)
						kv.replyChs[index] = replyCh

						kv.mu.Unlock()
						<-replyCh
						kv.migrateCond.Broadcast()
						kv.mu.Lock()*/
					}

				}
			} else if kv.dbstat.Config.Num == kv.config.Num+1 {
				raft.LogPrint(raft.INFO, dKvServer, "%s ticker config = %+v", kv.logPrefix, kv.config)
				if kv.dbstat.Stat == CONFIGING {
					dbstat := kv.newDbStat()
					dbstat.Config = kv.dbstat.Config.Copy()
					dbstat.Stat = CONFIGING

					op := Op{OP_CONFIG, CONFIG_CLIENT_ID, int64(kv.dbstat.Config.Num), dbstat}
					raft.LogPrint(raft.INFO, dKvServer, "%s num = %d ticker op = %+v", kv.logPrefix, kv.config.Num, op)
					index, term, _ := kv.rf.Start(op)

					opCache := &OpCache{term, index, op.Opcode, op.SeqId, false}
					kv.clientop[op.ClientId] = opCache
				}
			}
		}

		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}
