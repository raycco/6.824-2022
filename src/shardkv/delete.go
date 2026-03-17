package shardkv

import (
	"6.824/raft"
)

func (kv *ShardKV) processDeleteOp(op Op, term int, index int) OpReply {
	var opReply OpReply

	clientid := op.ClientId
	seqid := op.SeqId

	shard := op.Type.(int)

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process delete op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if (seqid >= opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
			kv.clientop[clientid] = opCache
			delete(kv.shardDbs, shard)
			kv.migratingCond.Broadcast()
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
		kv.clientop[clientid] = opCache
		delete(kv.shardDbs, shard)
		kv.migratingCond.Broadcast()
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d database %v",
		kv.logPrefix, index, kv.config.Num, kv.shardDbs)

	return opReply
}
