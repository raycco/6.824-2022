package shardkv

import (
	"bytes"
	"sort"
	"strconv"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	MIGRATE_CLIENT_ID = -1
	CONFIG_CLIENT_ID  = -2
	DELETE_CLIENT_ID  = -3
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

type MigrateArgs struct {
	Num   int
	Gid   int
	Shard int
	Data  []byte // raw bytes of the shard to migrate
}

type MigrateReply struct {
	Num   int
	Gid   int
	Shard int
	Err   Err
}

type MigrateProgressArgs struct {
	Shard      int
	IsComplete bool
}

type MigrateProgressReply struct {
	Err Err
}

type MigrateTask struct {
	num    int
	kvdata map[string]string
	cliseq map[int64]int64
}

func (src *MigrateTask) Copy() MigrateTask {
	var task MigrateTask
	task.num = src.num
	task.kvdata = make(map[string]string)
	for key, value := range src.kvdata {
		task.kvdata[key] = value
	}

	task.cliseq = make(map[int64]int64)
	for clientid, seqid := range src.cliseq {
		task.cliseq[clientid] = seqid
	}
	return task
}

type Migrate struct {
	Key    string
	Value  string
	CliSeq map[int64]int64
}

func (kv *ShardKV) doMigrateOp(op Op, term int, index int, migrate Migrate) OpReply {
	var opReply OpReply
	if migrate.Key == KEY_MIN || migrate.Key == KEY_MAX {
		shard, err := strconv.Atoi(migrate.Value)
		if err != nil {
			opReply = OpReply{ErrMigrating, ""}
		} else {
			kv.shardDbs[shard].LastKey = migrate.Key
			opReply = OpReply{OK, ""}
		}

		for clientid, seqid := range migrate.CliSeq {
			op, ok := kv.clientop[clientid]
			if !ok || op.SeqId <= seqid {
				kv.clientop[clientid] = &OpCache{term, index, OP_PUT, kv.config.Num, seqid, OK}
			}
		}

		if migrate.Key == KEY_MAX {
			kv.migratingCond.Broadcast()
		}
	} else {
		op.Type = KeyVal{migrate.Key, migrate.Value}
		opReply = kv.opExecute(op)
	}
	return opReply
}

func (kv *ShardKV) processMigrateOp(op Op, term int, index int) OpReply {
	clientid := op.ClientId
	seqid := op.SeqId

	migrate := op.Type.(Migrate)

	var opReply OpReply

	opCache, ok := kv.clientop[clientid]
	raft.LogPrint(raft.INFO, dKvServer, "%s index %d num %d process migrate op cache %v",
		kv.logPrefix, index, kv.config.Num, opCache)
	if ok {
		if (seqid >= opCache.SeqId && index >= opCache.Index) || opCache.Err == Empty {
			opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
			kv.clientop[clientid] = opCache
			opReply = kv.doMigrateOp(op, term, index, migrate)
		}
	} else {
		opCache = &OpCache{term, index, op.Opcode, kv.config.Num, seqid, OK}
		kv.clientop[clientid] = opCache
		opReply = kv.doMigrateOp(op, term, index, migrate)
	}

	return opReply
}

func (kv *ShardKV) sendMigratePush(srv *labrpc.ClientEnd, args *MigrateArgs, reply *MigrateReply) bool {
	ok := srv.Call("ShardKV.MigratePush", args, reply)
	return ok
}

func (kv *ShardKV) doPush(dstgid int, args *MigrateArgs, sharddb map[string]string) {
	if servers, ok := kv.config.Groups[dstgid]; ok {
		si, exist := kv.gidLeader[dstgid]
		if !exist {
			si = 0
		} else {
			si = kv.gidLeader[dstgid]
		}

		replyChan := make(chan Err)

		for !kv.killed() {
			srv := kv.make_end(servers[si])
			go func(srv *labrpc.ClientEnd, args *MigrateArgs) {
				raft.LogPrint(raft.INFO, dKvServer, "%s num = %d, push shard data %d => G%d-S%d args %v",
					kv.logPrefix, args.Num, args.Shard, dstgid, si, sharddb)
				var reply MigrateReply
				ok := kv.sendMigratePush(srv, args, &reply)
				if !ok {
					replyChan <- ErrReplyLost
				} else {
					replyChan <- reply.Err
				}
			}(srv, args)
			kv.mu.Unlock()
			err := <-replyChan
			kv.mu.Lock()

			raft.LogPrint(raft.INFO, dKvServer, "%s push shard data %d => G%d-S%d %s",
				kv.logPrefix, args.Shard, dstgid, si, err)
			if err == OK || err == ErrMigrateComplete {
				kv.gidLeader[dstgid] = si
				if err == ErrMigrateComplete {
					op := Op{OP_DELETE, kv.config.Num, DELETE_CLIENT_ID, int64(kv.config.Num), args.Shard}
					raft.LogPrint(raft.INFO, dKvServer, "%s pushed op = %+v", kv.logPrefix, op)
					kv.startOp(op)
				}
				break
			} else {
				si = (si + 1) % len(servers)
			}
		}
	}
}

func (kv *ShardKV) shardsMigrate(mode int, shards map[int]int) {

	switch mode {
	case MODE_PUSH:
		for shard, gid := range shards {
			shardDb, exist := kv.shardDbs[shard]
			sharddb := make(map[string]string)
			if exist && shardDb.LastKey != KEY_MAX {
				raft.LogPrint(raft.INFO, dKvServer, "%s num = %d, push data %+v",
					kv.logPrefix, kv.config.Num, shardDb)
				sharddb, _ = shardDb.From(shardDb.LastKey, 20)

				cliseq := make(map[int64]int64)
				for clientid, op := range kv.clientop {
					if clientid >= 0 && op.Err == OK {
						cliseq[clientid] = op.SeqId
					}
				}

				byteBuffer := new(bytes.Buffer)
				encoder := labgob.NewEncoder(byteBuffer)
				encoder.Encode(sharddb)
				encoder.Encode(cliseq)
				args := MigrateArgs{kv.config.Num, kv.gid, shard, byteBuffer.Bytes()}

				kv.doPush(gid, &args, sharddb)
			}
			delete(shards, shard)

			/*if servers, ok := kv.config.Groups[gid]; ok {
				si, exist := kv.gidLeader[gid]
				if !exist {
					si = 0
				} else {
					si = kv.gidLeader[gid]
				}

				for {
					srv := kv.make_end(servers[si])
					go func(srv *labrpc.ClientEnd, args *MigrateArgs) {
						raft.LogPrint(raft.INFO, dKvServer, "%s num = %d, push data migrate shards %d => G%d-S%d args %v",
							kv.logPrefix, args.Num, shards, gid, si, sharddb)
						var reply MigrateReply
						ok := srv.Call("ShardKV.MigratePush", args, &reply)
						replyChan <- PushReply{ok, reply.Err}
					}(srv, &args)
					kv.mu.Unlock()
					pushReply := <-replyChan
					kv.mu.Lock()

					if pushReply.ok {
						raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d %s",
							kv.logPrefix, shards, gid, si, pushReply.Err)
						if pushReply.Err == OK || pushReply.Err == ErrMigrateComplete {
							kv.gidLeader[gid] = si
							delete(shards, shard)
							if pushReply.Err == ErrMigrateComplete {
								op := Op{OP_DELETE, kv.config.Num, DELETE_CLIENT_ID, int64(kv.config.Num), shard}
								raft.LogPrint(raft.INFO, dKvServer, "%s pushed op = %+v", kv.logPrefix, op)
								kv.startOp(op)
							}
							break
						} else {
							si = (si + 1) % len(servers)
						}
					} else {
						si = (si + 1) % len(servers)
						raft.LogPrint(raft.INFO, dKvServer, "%s push data migrate shards %d => G%d-S%d fail", kv.logPrefix, shards, gid, si)
					}
				}
			}*/
		}
	case MODE_PULL:
		// todo
	default:
		raft.LogPrint(raft.INFO, dKvServer, "%s num = %d mode = %d unsupport", kv.logPrefix, kv.config.Num, mode)
	}
}

func (kv *ShardKV) MigratePush(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request, local num = %d, args num = %d",
		kv.logPrefix, isLeader, kv.config.Num, args.Num)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if args.Num == kv.config.Num {

		if _, pushed := kv.migrateTasks[args.Shard]; pushed {
			reply.Err = OK
			return
		}

		byteBuffer := bytes.NewBuffer(args.Data)
		decoder := labgob.NewDecoder(byteBuffer)

		var task MigrateTask
		task.num = args.Num
		task.kvdata = make(map[string]string)
		task.cliseq = make(map[int64]int64)
		decoder.Decode(&task.kvdata)
		decoder.Decode(&task.cliseq)

		sharddb, exist := kv.shardDbs[args.Shard]
		if exist && sharddb.LastKey != KEY_MIN {
			if sharddb.LastKey == KEY_MAX {
				reply.Err = OK
				return
			} else {
				for key := range task.kvdata {
					if key <= sharddb.LastKey {
						delete(task.kvdata, key)
					}
				}
			}
		}

		raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request args %v", kv.logPrefix, isLeader, task.kvdata)

		kv.migrateTasks[args.Shard] = task

		reply.Err = OK
	} else if args.Num < kv.config.Num {
		reply.Err = ErrMigrateComplete
	} else {
		reply.Err = ErrMigrating
	}
}

func (kv *ShardKV) MigrateProgress(args *MigrateProgressArgs, reply *MigrateProgressReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if isLeader {
		if args.IsComplete {
			op := Op{OP_DELETE, kv.config.Num, DELETE_CLIENT_ID, int64(kv.config.Num), args.Shard}
			kv.startOp(op)
			raft.LogPrint(raft.INFO, dKvServer, "%s receive migrate complete", kv.logPrefix)
		}
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) MigratePull(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//todo
	reply.Err = OK
}

func (kv *ShardKV) migrater() {

	for !kv.killed() {
		kv.mu.Lock()

		for shard, task := range kv.migrateTasks {

			keys := make([]string, 0)
			for key := range task.kvdata {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			op := Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), Migrate{KEY_MIN, strconv.Itoa(shard), task.cliseq}}
			raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
			opReply := kv.startOp(op)
			raft.LogPrint(raft.INFO, dKvServer, "%s migrate op reply = %+v", kv.logPrefix, opReply)

			for _, key := range keys {
				op = Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), Migrate{key, task.kvdata[key], nil}}
				raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
				opReply = kv.startOp(op)
				raft.LogPrint(raft.INFO, dKvServer, "%s migrate op reply = %+v", kv.logPrefix, opReply)

				kv.migratingCond.Broadcast()
			}

			op = Op{OP_MIGRATE, kv.config.Num, MIGRATE_CLIENT_ID, int64(kv.config.Num), Migrate{KEY_MAX, strconv.Itoa(shard), nil}}
			raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
			opReply = kv.startOp(op)
			raft.LogPrint(raft.INFO, dKvServer, "%s migrate op reply = %+v", kv.logPrefix, opReply)

			replyChan := make(chan Err)

			var cfg Cfg
			if kv.lastConfig.Num != task.num-1 {
				cfg = Cfg(kv.cfgck.Query(task.num - 1))
			} else {
				cfg = kv.lastConfig
			}
			srcgid := cfg.Shards[shard]
			if servers, ok := cfg.Groups[srcgid]; ok {
				success := false
				for !kv.killed() {
					for si := 0; si < len(servers); si++ {

						var args MigrateProgressArgs
						args.IsComplete = true
						args.Shard = shard
						srv := kv.make_end(servers[si])

						go func(srv *labrpc.ClientEnd, args *MigrateProgressArgs) {
							var reply MigrateProgressReply
							raft.LogPrint(raft.INFO, dKvServer, "%s send migrate complete args %+v", kv.logPrefix, args)
							ok := srv.Call("ShardKV.MigrateProgress", args, &reply)
							if !ok {
								replyChan <- ErrReplyLost
							} else {
								replyChan <- reply.Err
							}
						}(srv, &args)
						kv.mu.Unlock()
						err := <-replyChan
						kv.mu.Lock()

						raft.LogPrint(raft.INFO, dKvServer, "%s migrate complete reply %+v => G%d-S%d", kv.logPrefix, err, srcgid, si)

						if err == OK {
							success = true
							break
						}

					}

					if success {
						break
					}
				}
			}

			delete(kv.migrateTasks, shard)
		}
		kv.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}
