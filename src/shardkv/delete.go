package shardkv

import (
	"6.824/raft"
)

func (kv *ShardKV) processDeleteOp(op Op, term int, index int, isleader bool) OpReply {
	var opReply OpReply

	dbstat := op.Type.(DbStat)
	kv.dbstat = dbstat.Copy()

	clientid := op.ClientId
	seqid := op.SeqId

	kv.lastConfig = Cfg(kv.cfgck.Query(kv.config.Num - 1))
	raft.LogPrint(raft.INFO, dKvServer, "%s prepare data delete %v => %v",
		kv.logPrefix, kv.lastConfig.Shards, kv.config.Shards)

	gidShardsSrcMap := make(map[int][]int)
	gidShardsDstMap := make(map[int][]int)
	for shard := 0; shard < len(kv.config.Shards); shard++ {
		ngid := kv.config.Shards[shard]
		ogid := kv.lastConfig.Shards[shard]
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

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process delete op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if (seqid >= opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
			kv.clientop[clientid] = opCache
			if dbstat.LastKey == "MAX" {
				kv.migratingCond.Signal()
			}
			kv.deleteShards(gidShardsDstMap)
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
		kv.clientop[clientid] = opCache
		if dbstat.LastKey == "MAX" {
			kv.migratingCond.Signal()
		}
		kv.deleteShards(gidShardsDstMap)
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d database %v",
		kv.logPrefix, index, kv.config.Num, kv.database)

	//kv.createSnapshot(true)

	return opReply
}

func (kv *ShardKV) deleteShards(gidShards map[int][]int) {
	for _, shards := range gidShards {
		for _, shard := range shards {
			for key := range kv.database {
				if shard == key2shard(key) {
					delete(kv.database, key)
				}
			}
		}
	}
}
