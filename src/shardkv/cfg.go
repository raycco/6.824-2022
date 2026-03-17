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

func (kv *ShardKV) processConfigOp(op Op, term int, index int) OpReply {

	var opReply OpReply

	newcfg := op.Type.(Cfg)

	if kv.config.Num+1 != newcfg.Num {
		opReply = OpReply{ErrConfigChange, ""}
		return opReply
	}

	clientid := op.ClientId
	seqid := op.SeqId

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process config op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if (seqid > opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
			kv.clientop[clientid] = opCache
			kv.lastConfig = kv.config
			kv.config = newcfg
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
		kv.clientop[clientid] = opCache
		kv.lastConfig = kv.config
		kv.config = newcfg
	}

	srcShards, dstShards := kv.calcMigrateShards()
	raft.LogPrint(raft.INFO, dKvServer, "%s process config op srcgids %+v dstgids %+v database %+v",
		kv.logPrefix, srcShards, dstShards, kv.shardDbs)

	for shard := range srcShards {
		kv.shardDbs[shard] = NewShardDb()
		if kv.lastConfig.Num != 0 {
			kv.shardDbs[shard].LastKey = KEY_MIN
		}
	}

	for shard := range dstShards {
		kv.shardDbs[shard].LastKey = KEY_MIN
	}

	opReply = OpReply{OK, ""}

	return opReply
}

func (kv *ShardKV) calcMigrateShards() (map[int]int, map[int]int) {
	if kv.lastConfig.Num != kv.config.Num-1 {
		kv.lastConfig = Cfg(kv.cfgck.Query(kv.config.Num - 1))
	}
	oldcfg := kv.lastConfig
	newcfg := kv.config

	raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate %v => %v",
		kv.logPrefix, oldcfg.Shards, newcfg.Shards)

	srcShards := make(map[int]int)
	dstShards := make(map[int]int)
	for shard := 0; shard < len(newcfg.Shards); shard++ {
		ngid := newcfg.Shards[shard]
		ogid := oldcfg.Shards[shard]
		if ogid != ngid {
			if ogid == kv.gid { // push
				// start migrating the data for that shard to the replica group that is taking over ownership
				//dstGidShards[ngid] = append(dstGidShards[ngid], shard)
				dstShards[shard] = ngid
			} else if ngid == kv.gid { // pull
				// wait for the previous owner to send over the old shard data
				//srcGidShards[ogid] = append(srcGidShards[ogid], shard)
				srcShards[shard] = ogid
			} else {
				// other group need wait
			}
		}
	}

	return srcShards, dstShards
}

func (kv *ShardKV) kvState() int {
	srcShards, dstShards := kv.calcMigrateShards()
	for shard, db := range kv.shardDbs {
		_, ok1 := srcShards[shard]
		if ok1 && db.LastKey != KEY_MAX {
			return MIGRATING
		}

		_, ok2 := dstShards[shard]
		if ok2 && db.LastKey != KEY_MAX {
			return PUSHING
		}
	}

	return SERVING
}

func (kv *ShardKV) configer() {
	for !kv.killed() {

		kv.mu.Lock()

		_, isLeader := kv.rf.GetState()
		if isLeader {
			state := kv.kvState()
			//raft.LogPrint(raft.INFO, dKvServer, "%s configer num = %d, state = %d", kv.logPrefix, kv.config.Num, state)
			switch state {
			case SERVING:
				newcfg := kv.cfgck.Query(kv.config.Num + 1)
				if newcfg.Num == kv.config.Num+1 {
					op := Op{OP_CONFIG, newcfg.Num, CONFIG_CLIENT_ID, int64(newcfg.Num), Cfg(newcfg)}
					raft.LogPrint(raft.INFO, dKvServer, "%s configer num = %d, newcfg = %+v", kv.logPrefix, kv.config.Num, newcfg)
					kv.startOp(op)
				}
			case PUSHING:
				_, dstShards := kv.calcMigrateShards()
				if len(dstShards) > 0 {
					for {
						success := kv.shardsMigrate(MODE_PUSH, dstShards)
						if success {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
				} else {

				}

			case MIGRATING:
				raft.LogPrint(raft.INFO, dKvServer, "%s num = %d MIGRATING", kv.logPrefix, kv.config.Num)
			}
		}
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}
