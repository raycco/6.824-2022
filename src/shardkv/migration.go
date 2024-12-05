package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

type RequestMigrateArgs struct {
	Ogid int
	Ngid int
	Num  int
	Data []byte // raw bytes of the shard to migrate
}

type RequestMigrateReply RequestMigrateArgs

func (kv *ShardKV) prepareMigration(config shardctrler.Config) (map[int][]int, map[int][]int) {
	raft.LogPrint(raft.INFO, dKvServer, "%s prepare data migrate %v => %v", kv.logPrefix, kv.config.Shards, config.Shards)

	gidShardsSrcMap := make(map[int][]int)
	gidShardsDstMap := make(map[int][]int)
	for shard := 0; shard < len(config.Shards); shard++ {
		ngid := config.Shards[shard]
		ogid := kv.config.Shards[shard]
		if ogid != ngid {
			if ogid == kv.gid {
				// start migrating the data for that shard to the replica group that is taking over ownership
				gidShardsDstMap[ngid] = append(gidShardsDstMap[ngid], shard)
			} else if ngid == kv.gid {
				//  wait for the previous owner to send over the old shard data
				gidShardsSrcMap[ogid] = append(gidShardsSrcMap[ogid], shard)
			} else {
				// other group need wait
			}
		}
	}

	return gidShardsSrcMap, gidShardsDstMap
}

func (kv *ShardKV) dataMigration(gidShardsDstMap map[int][]int, config shardctrler.Config) {

	for gid, shards := range gidShardsDstMap {
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
		args.Ogid = kv.gid
		args.Ngid = gid
		args.Num = config.Num
		if servers, ok := config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				var reply RequestMigrateReply
				srv := kv.make_end(servers[si])
				raft.LogPrint(raft.INFO, dKvServer, "%s data migrate shards %d gid %d => %d args %v", kv.logPrefix, shards, kv.gid, gid, sharddb)
				ok := srv.Call("ShardKV.Migrate", &args, &reply)
				if ok {
					//raft.LogPrint(raft.INFO, dKvServer, "%s shard %d gid %d => %d reply %v", kv.logPrefix, shard, kv.gid, gid, reply)
				}
			}
		}
	}
}

func (kv *ShardKV) Migrate(args *RequestMigrateArgs, reply *RequestMigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	byteBuffer := bytes.NewBuffer(args.Data)
	decoder := labgob.NewDecoder(byteBuffer)

	sharddb := make(map[string]string)

	decoder.Decode(&sharddb)

	_, isLeader := kv.rf.GetState()

	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive migrate data args %v", kv.logPrefix, isLeader, sharddb)

	for key, value := range sharddb {
		kv.database[key] = value
	}

	raft.LogPrint(raft.INFO, dKvServer, "%s receive migrate data snapshot LII=%d database=%v", kv.logPrefix, kv.lastIncludedIndex, kv.database)

	if kv.lastIncludedIndex > 0 {
		byteBufferSnap := new(bytes.Buffer)
		encoder := labgob.NewEncoder(byteBufferSnap)
		encoder.Encode(kv.lastIncludedIndex)
		encoder.Encode(kv.database)
		encoder.Encode(kv.clientop)
		kv.rf.Snapshot(kv.lastIncludedIndex, byteBufferSnap.Bytes())
	}

	if isLeader {
		raft.LogPrint(raft.INFO, dKvServer, "%s data migrate signal", kv.logPrefix)
		kv.mu.Unlock()
		kv.migrateCh <- true
		kv.mu.Lock()
		raft.LogPrint(raft.INFO, dKvServer, "%s data migrate send signal", kv.logPrefix)
	}

}
