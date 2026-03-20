package shardkv

import (
	"6.824/raft"
)

func (kv *ShardKV) processDeleteOp(op Op, index int) OpReply {
	shard := op.Type.(int)
	return kv.processInternalOp(op, index, func() OpReply {
		// TestUnreliable3删除操作可能比配置升级操作之后apply，
		// 需要忽略上一个版本的delete操作
		if op.Num >= kv.currCfg.Num {
			delete(kv.shardDbs, shard)
		}
		kv.migratingCond.Broadcast()
		raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d database %v",
			kv.logPrefix, index, kv.currCfg.Num, kv.shardDbs)
		return OpReply{}
	})
}

func (kv *ShardKV) processNoOp(op Op, index int) OpReply {
	return kv.processInternalOp(op, index, func() OpReply {
		raft.LogPrint(raft.INFO, dKvServer, "%s no-op", kv.logPrefix)
		return OpReply{}
	})
}
