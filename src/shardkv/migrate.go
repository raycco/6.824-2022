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

const MigrateCheckTimeInterval = 50 * time.Millisecond
const SrvSwitchTimeInterval = 10 * time.Millisecond
const MigrateKVCount = 32

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
	Num        int
	Shard      int
	IsComplete bool
}

type MigrateProgressReply struct {
	Err Err
}

type MigrateTask struct {
	num    int
	kvData map[string]string
	cliSeq map[int64]int64
}

func (src *MigrateTask) Copy() MigrateTask {
	var dst MigrateTask
	dst.num = src.num
	dst.kvData = make(map[string]string)
	for key, value := range src.kvData {
		dst.kvData[key] = value
	}

	dst.cliSeq = make(map[int64]int64)
	for clientid, seqid := range src.cliSeq {
		dst.cliSeq[clientid] = seqid
	}
	return dst
}

type Migrate struct {
	Key    string
	Value  string
	CliSeq map[int64]int64
}

func (kv *ShardKV) doMigrateOp(op Op, index int, migrate Migrate) OpReply {
	var opReply OpReply
	if migrate.Key == KEY_MIN || migrate.Key == KEY_MAX {
		shard, err := strconv.Atoi(migrate.Value)
		if err != nil {
			opReply = OpReply{ErrDataMigrate, ""}
		} else {
			db := kv.shardDbs[shard]
			db.SetLastKey(migrate.Key)

			opReply = OpReply{OK, ""}
		}

		for cliId, seqId := range migrate.CliSeq {
			op, ok := kv.clientOp[cliId]
			if !ok || op.SeqId <= seqId {
				kv.clientOp[cliId] = &OpCache{index, OP_PUT, kv.currCfg.Num, seqId, OK}
			}
		}
	} else {
		op.Type = KeyVal{migrate.Key, migrate.Value}
		opReply = kv.opExecute(op)
	}
	return opReply
}

func (kv *ShardKV) processMigrateOp(op Op, index int) OpReply {
	migrate := op.Type.(Migrate)
	return kv.processInternalOp(op, index, func() OpReply {
		return kv.doMigrateOp(op, index, migrate)
	})
}

func (kv *ShardKV) sendMigratePush(srv *labrpc.ClientEnd, args *MigrateArgs, reply *MigrateReply) bool {
	ok := srv.Call("ShardKV.MigratePush", args, reply)
	return ok
}

func (kv *ShardKV) doPush(dstgid int, args *MigrateArgs, sharddb map[string]string) {
	if servers, ok := kv.currCfg.Groups[dstgid]; ok {
		si := kv.gidLeaderId[dstgid]

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
				kv.gidLeaderId[dstgid] = si
				if err == ErrMigrateComplete {
					op := Op{OP_DELETE, kv.currCfg.Num, DELETE_CLIENT_ID, int64(kv.currCfg.Num), args.Shard}
					raft.LogPrint(raft.INFO, dKvServer, "%s pushed op = %+v", kv.logPrefix, op)
					kv.startOp(op)
				}
				break
			} else {
				si = (si + 1) % len(servers)
				time.Sleep(SrvSwitchTimeInterval)
			}
		}
	}
}

func (kv *ShardKV) shardsMigrate(mode int, shards map[int]int) {

	switch mode {
	case MODE_PUSH:
		for shard, gid := range shards {
			db, exist := kv.shardDbs[shard]
			kvData := make(map[string]string)
			if exist && !db.IsLastKeyMax() {
				raft.LogPrint(raft.INFO, dKvServer, "%s num = %d, push shard db %+v",
					kv.logPrefix, kv.currCfg.Num, db)
				kvData, _ = db.From(db.GetLastKey(), MigrateKVCount)

				cliSeq := make(map[int64]int64)
				for cliId, op := range kv.clientOp {
					if cliId >= 0 && op.Err == OK {
						cliSeq[cliId] = op.SeqId
					}
				}

				byteBuffer := new(bytes.Buffer)
				encoder := labgob.NewEncoder(byteBuffer)
				encoder.Encode(kvData)
				encoder.Encode(cliSeq)
				args := MigrateArgs{kv.currCfg.Num, kv.gid, shard, byteBuffer.Bytes()}

				kv.doPush(gid, &args, kvData)
			}
			delete(shards, shard)
		}
	case MODE_PULL:
		// todo
	default:
		raft.LogPrint(raft.INFO, dKvServer, "%s num = %d mode = %d unsupport", kv.logPrefix, kv.currCfg.Num, mode)
	}
}

func (kv *ShardKV) MigratePush(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data local num = %d, args num = %d shard = %d",
		kv.logPrefix, isLeader, kv.currCfg.Num, args.Num, args.Shard)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if args.Num == kv.currCfg.Num {

		if _, pushed := kv.migrateTasks[args.Shard]; pushed {
			reply.Err = OK
			return
		}

		byteBuffer := bytes.NewBuffer(args.Data)
		decoder := labgob.NewDecoder(byteBuffer)

		task := MigrateTask{args.Num, make(map[string]string), make(map[int64]int64)}
		decoder.Decode(&task.kvData)
		decoder.Decode(&task.cliSeq)

		// 去重，当迁移到一半时，group killed，重启后会推送重复数据
		// 情形一：KEY_MAX需单独处理不能作为Key大小比较
		// 如configer协程刚好start op还未apply，此时收到旧的已经迁移的数据

		// 情形二：支持空map[]进入迁移任务队列
		// 1.重启group，shard 0 &{KvData:map[2:_] Keys:[2] LastKey:2}，index 120将LastKey设置为KEY_MAX未apply
		// 2.接收到shard 0的重复数据map[2:_]，由于已经迁移到key=2，则将map[]存入迁移队列
		// 3.apply index 120将LastKey设置为KEY_MAX，此时num = 14
		// 4.configer协程拉取新配置shard 0需要迁移，产生index 122更新配置
		// 5.migrater协程开始执行map[]迁移，产生index 123
		// 6.先后apply index 122与123，将shard 0将LastKey设置KEY_MIN
		// 7.migrater协程迁移完成，产生index 124，将LastKey设置为KEY_MAX
		// 8.shard 0将不会迁出，导致阻塞无法继续执行
		// 9.若map[]不添加到迁移队列直接回复OK，推送方将有可能收不到进度

		// 情形三：apply与重复推送的异步执行
		// 1. 新组，Key=7只是提交了Raft：index 6，即LastKey为KEY_MIN，新组与旧组同时killed
		// 2. 重启后，在index 6 apply之前，收到旧组推过来的数据存入迁移任务队列
		// 3. apply index 6，shard 5 &{KvData:map[7:e] Keys:[7] LastKey:7}
		// 4. 接收客户端的请求，提交Raft：index 7，apply操作成功：shard 5 &{KvData:map[7:ei] Keys:[7] LastKey:7}
		// 5. 再次执行迁移任务队列的任务，shard 5数据最终变成&{KvData:map[7:e] Keys:[7] LastKey:=}，数据被覆盖

		if db, ok := kv.shardDbs[args.Shard]; ok {
			if db.IsLastKeyMax() {
				reply.Err = OK
				return
			}

			for key := range task.kvData {
				if key <= db.GetLastKey() {
					delete(task.kvData, key)
				}
			}
		}

		if len(task.kvData) == 0 {
			// 因为目前是整个shard推送，所以直接回复迁移完成
			// 如果要支持shard数据部分推送，还需设计一个合理方案
			succ := kv.startMigrateOp(KEY_MAX, strconv.Itoa(args.Shard), nil)
			if succ {
				reply.Err = ErrMigrateComplete
			} else {
				reply.Err = OK
			}
			return
		}

		raft.LogPrint(raft.INFO, dKvServer, "%s Leader=%v receive push data request args %v",
			kv.logPrefix, isLeader, task.kvData)

		kv.migrateTasks[args.Shard] = task
		reply.Err = OK
	} else if args.Num < kv.currCfg.Num {
		reply.Err = ErrMigrateComplete
	} else {
		reply.Err = ErrDataMigrate
	}
}

func (kv *ShardKV) MigrateProgress(args *MigrateProgressArgs, reply *MigrateProgressReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if isLeader {
		if args.IsComplete {
			op := Op{OP_DELETE, args.Num, DELETE_CLIENT_ID, int64(args.Num), args.Shard}
			kv.startOp(op)
			raft.LogPrint(raft.INFO, dKvServer, "%s receive migrate complete %+v", kv.logPrefix, args)
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

func (kv *ShardKV) sendMigrateProgress(srv *labrpc.ClientEnd, args *MigrateProgressArgs, reply *MigrateProgressReply) bool {
	ok := srv.Call("ShardKV.MigrateProgress", args, reply)
	return ok
}

func (kv *ShardKV) doProgress(shard int, num int) {
	oldCfg := kv.lastCfg
	if kv.lastCfg.Num != num-1 {
		oldCfg = Cfg(kv.cfgck.Query(num - 1))
	}

	replyChan := make(chan Err)

	srcgid := oldCfg.Shards[shard]
	if servers, ok := oldCfg.Groups[srcgid]; ok {
		si := kv.gidLeaderId[srcgid]

		args := MigrateProgressArgs{kv.currCfg.Num, shard, true}

		for !kv.killed() {

			srv := kv.make_end(servers[si])
			go func(srv *labrpc.ClientEnd, args *MigrateProgressArgs) {
				var reply MigrateProgressReply
				raft.LogPrint(raft.INFO, dKvServer, "%s send migrate complete args %+v", kv.logPrefix, args)
				ok := kv.sendMigrateProgress(srv, args, &reply)
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
				// 尽可能快的退出循环，否则会阻塞migrater，影响迁移（TestChallenge2Unaffected）
				kv.gidLeaderId[srcgid] = si
				break
			} else {
				si = (si + 1) % len(servers)
				time.Sleep(SrvSwitchTimeInterval)
			}
		}
	}
}

func (kv *ShardKV) startMigrateOp(key string, value string, cliSeq map[int64]int64) bool {

	op := Op{
		Opcode:   OP_MIGRATE,
		Num:      kv.currCfg.Num,
		ClientId: MIGRATE_CLIENT_ID,
		SeqId:    int64(kv.currCfg.Num),
		Type:     Migrate{key, value, cliSeq},
	}
	raft.LogPrint(raft.INFO, dKvServer, "%s migrate op = %+v", kv.logPrefix, op)
	opReply := kv.startOp(op)
	raft.LogPrint(raft.INFO, dKvServer, "%s migrate op reply = %+v", kv.logPrefix, opReply)
	if opReply.Err != OK {
		return false
	}
	return true
}

func (kv *ShardKV) migrater() {

	for !kv.killed() {
		kv.mu.Lock()

		ok := true
		for shard, task := range kv.migrateTasks {

			// 情形三：apply与重复推送的异步执行
			if db, exist := kv.shardDbs[shard]; exist {
				for key := range task.kvData {
					if key <= db.GetLastKey() {
						delete(task.kvData, key)
					}
				}
			}

			if len(task.kvData) > 0 {
				keys := make([]string, 0)
				for key := range task.kvData {
					keys = append(keys, key)
				}
				sort.Strings(keys)

				ok = kv.startMigrateOp(KEY_MIN, strconv.Itoa(shard), task.cliSeq)

				for _, key := range keys {
					if ok = kv.startMigrateOp(key, task.kvData[key], nil); !ok {
						break
					}
				}
			}

			if ok {
				if ok = kv.startMigrateOp(KEY_MAX, strconv.Itoa(shard), nil); ok {
					// 有可能leader切换，迁移失败，此时不能更新进度
					kv.doProgress(shard, task.num)
				}
			}

			delete(kv.migrateTasks, shard)
		}
		kv.mu.Unlock()

		time.Sleep(MigrateCheckTimeInterval)
	}
}
