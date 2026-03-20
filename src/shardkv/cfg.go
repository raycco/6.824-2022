package shardkv

import (
	"time"

	"6.824/raft"
	"6.824/shardctrler"
)

const ConfigCheckTimeInterval = 100 * time.Millisecond

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

func (kv *ShardKV) processConfigOp(op Op, index int) OpReply {

	var opReply OpReply

	newCfg := op.Type.(Cfg)

	if kv.currCfg.Num+1 != newCfg.Num {
		opReply = OpReply{ErrConfigChange, ""}
		return opReply
	}

	kv.processInternalOp(op, index, func() OpReply {
		kv.lastCfg = kv.currCfg
		kv.currCfg = newCfg
		return OpReply{}
	})

	srcShards, dstShards := kv.calcMigrateShards()
	raft.LogPrint(raft.INFO, dKvServer, "%s process config op srcgids %+v dstgids %+v database %+v",
		kv.logPrefix, srcShards, dstShards, kv.shardDbs)

	for shard := range srcShards {
		kv.shardDbs[shard] = NewShardDb()
		if kv.lastCfg.Num != 0 {
			kv.shardDbs[shard].SetLastKey(KEY_MIN)
		}
	}

	for shard := range dstShards {
		kv.shardDbs[shard].SetLastKey(KEY_MIN)
	}

	opReply = OpReply{OK, ""}

	return opReply
}

func (kv *ShardKV) calcMigrateShards() (map[int]int, map[int]int) {

	oldCfg := kv.lastCfg
	newCfg := kv.currCfg

	raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate %v => %v",
		kv.logPrefix, oldCfg.Shards, newCfg.Shards)

	srcShards := make(map[int]int)
	dstShards := make(map[int]int)
	for shard := 0; shard < len(newCfg.Shards); shard++ {
		ngid := newCfg.Shards[shard]
		ogid := oldCfg.Shards[shard]
		if ogid != ngid {
			if ogid == kv.gid { // push
				//dstGidShards[ngid] = append(dstGidShards[ngid], shard)
				dstShards[shard] = ngid
			} else if ngid == kv.gid { // pull
				//srcGidShards[ogid] = append(srcGidShards[ogid], shard)
				srcShards[shard] = ogid
			} else {
				// other group need wait
			}
		}
	}

	return srcShards, dstShards
}

func (kv *ShardKV) kvState() {

	if kv.currCfg.Num > 0 && kv.lastCfg.Num != kv.currCfg.Num-1 {
		kv.lastCfg = Cfg(kv.cfgck.Query(kv.currCfg.Num - 1))
	}

	oldCfg := kv.lastCfg
	newCfg := kv.currCfg

	for shard, db := range kv.shardDbs {
		if kv.gid == newCfg.Shards[shard] && !db.IsLastKeyMax() {
			kv.state = MIGRATING
			return
		}

		if kv.gid == oldCfg.Shards[shard] && !db.IsLastKeyMax() {
			kv.state = PUSHING
			return
		}
	}

	kv.state = SERVING
}

func (kv *ShardKV) configer() {
	for !kv.killed() {
		kv.mu.Lock()

		_, isLeader := kv.rf.GetState()
		if isLeader {
			if kv.state == ACTIVING {
				// 102-S0 log index 265即将shard的LastKey设置为KEY_MAX，
				// 但是102-S1与102-S2 commit index到264，此时kill server，
				// 重启后leader变为S1，265不会apply，102一直处于MIGRATING
				kv.state = SERVING
				op := Op{Opcode: OP_NONE}
				kv.startOp(op)
			}

			kv.kvState()
			switch kv.state {
			case SERVING:
				newCfg := kv.cfgck.Query(kv.currCfg.Num + 1)
				if newCfg.Num == kv.currCfg.Num+1 {
					op := Op{OP_CONFIG, newCfg.Num, CONFIG_CLIENT_ID, int64(newCfg.Num), Cfg(newCfg)}
					raft.LogPrint(raft.INFO, dKvServer, "%s configer num = %d, new config = %+v",
						kv.logPrefix, kv.currCfg.Num, newCfg)
					kv.startOp(op)
				}
			case PUSHING:
				_, dstShards := kv.calcMigrateShards()
				if len(dstShards) > 0 {
					kv.shardsMigrate(MODE_PUSH, dstShards)
				}
			case MIGRATING:
				raft.LogPrint(raft.INFO, dKvServer, "%s num = %d data is migrating", kv.logPrefix, kv.currCfg.Num)
			}
		}
		kv.mu.Unlock()

		time.Sleep(ConfigCheckTimeInterval)
	}
}
