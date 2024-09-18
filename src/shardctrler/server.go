package shardctrler

import (
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	configs []Config // indexed by config num

	lastApplied int

	clientop map[int64]*OpCache   // client id -> last op
	replyChs map[int]chan OpReply // index -> reply channel
}

const (
	OP_JOIN  = 1
	OP_LEAVE = 2
	OP_MOVE  = 3
	OP_QUERY = 4
)

type Op struct {
	// Your data here.
	Opcode int
	Args   interface{}
}

type OpReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type OpCache struct {
	Term   int
	Index  int
	Opcode int
	SeqId  int64
}

func (sc *ShardCtrler) buildReply(isLeader bool) OpReply {
	var opReply OpReply
	if !isLeader {
		opReply.WrongLeader = true
		opReply.Err = ErrWrongLeader
	} else {
		opReply.WrongLeader = false
		opReply.Err = OK
	}

	return opReply
}

func (sc *ShardCtrler) getFieldOfArgs(args interface{}, fieldName string) int64 {
	value := reflect.ValueOf(args)
	//value = value.Elem()

	field := value.FieldByName(fieldName)
	return field.Int()
}

func (sc *ShardCtrler) processRequest(op Op) OpReply {

	raft.LogPrint(raft.INFO, dScServer, "S%d process request %+v", sc.me, op)
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		return sc.buildReply(false)
	}

	clientid := sc.getFieldOfArgs(op.Args, "ClientId")
	seqid := sc.getFieldOfArgs(op.Args, "SeqId")

	if op.Opcode != OP_QUERY {
		opCache, ok := sc.clientop[clientid]
		if ok && seqid < opCache.SeqId {
			return sc.buildReply(true)
		}
	}

	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		return sc.buildReply(false)
	} else {
		opCache := &OpCache{term, index, op.Opcode, seqid}
		sc.clientop[clientid] = opCache

		replyCh := make(chan OpReply)
		sc.replyChs[index] = replyCh

		sc.mu.Unlock()
		opReply := <-replyCh
		raft.LogPrint(raft.INFO, dScServer, "S%d send response to C%d reply %+v", sc.me, clientid, opReply)
		sc.mu.Lock()
		return opReply
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	raft.LogPrint(raft.INFO, dScServer, "S%d receive Join request from C%d args %+v", sc.me, args.ClientId, args)
	op := Op{OP_JOIN, *args}
	opReply := sc.processRequest(op)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	raft.LogPrint(raft.INFO, dScServer, "S%d receive Leave request from C%d args %+v", sc.me, args.ClientId, args)
	op := Op{OP_LEAVE, *args}
	opReply := sc.processRequest(op)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	raft.LogPrint(raft.INFO, dScServer, "S%d receive Move request from C%d args %+v", sc.me, args.ClientId, args)
	op := Op{OP_MOVE, *args}
	opReply := sc.processRequest(op)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	raft.LogPrint(raft.INFO, dScServer, "S%d receive Query request from C%d args %+v", sc.me, args.ClientId, args)
	op := Op{OP_QUERY, *args}
	opReply := sc.processRequest(op)
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	reply.Config.Num = opReply.Config.Num
	reply.Config.Shards = opReply.Config.Shards
	reply.Config.Groups = make(map[int][]string)
	for gid, servers := range opReply.Config.Groups {
		reply.Config.Groups[gid] = servers
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh

		sc.mu.Lock()
		raft.LogPrint(raft.INFO, dScServer, "S%d receive apply msg %+v", sc.me, applyMsg)
		if applyMsg.CommandIndex <= sc.lastApplied {
			continue
		} else {
			sc.lastApplied = applyMsg.CommandIndex
		}

		op := applyMsg.Command.(Op)
		index := applyMsg.CommandIndex
		term, isLeader := sc.rf.GetState()

		var opReply OpReply
		clientid := sc.getFieldOfArgs(op.Args, "ClientId")
		seqid := sc.getFieldOfArgs(op.Args, "SeqId")

		opCache, ok := sc.clientop[clientid]
		if ok {
			if op.Opcode == OP_QUERY {
				opReply = sc.opExecute(op, isLeader)
				if seqid >= opCache.SeqId {
					opCache = &OpCache{term, index, op.Opcode, seqid}
				}
			} else {
				if seqid >= opCache.SeqId && index >= opCache.Index {
					opReply = sc.opExecute(op, isLeader)
					opCache = &OpCache{term, index, op.Opcode, seqid}
				} else {
					opReply = sc.buildReply(isLeader)
				}
			}
		} else {
			opReply = sc.opExecute(op, isLeader)
			opCache = &OpCache{term, index, op.Opcode, seqid}
		}

		if isLeader {
			if replyCh, ok := sc.replyChs[index]; ok && replyCh != nil {
				sc.mu.Unlock()
				replyCh <- opReply
				sc.mu.Lock()

				close(replyCh)
				replyCh = nil
				delete(sc.replyChs, index)
			}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) opExecute(op Op, isLeader bool) OpReply {

	opReply := sc.buildReply(isLeader)
	switch op.Opcode {
	case OP_JOIN:
		args := op.Args.(JoinArgs)
		sc.opJoinExec(args)

	case OP_LEAVE:
		args := op.Args.(LeaveArgs)
		sc.opLeaveExec(args)

	case OP_MOVE:
		args := op.Args.(MoveArgs)
		sc.opMoveExec(args)

	case OP_QUERY:
		args := op.Args.(QueryArgs)
		opReply.Config = sc.opQueryExec(args)

	default:
		raft.LogPrint(raft.ERROR, dScServer, "S%d unknown operation")
		opReply = OpReply{}
	}

	return opReply
}

func (sc *ShardCtrler) min(x, y int) int {

	if x > y {
		return y
	} else {
		return x
	}
}

// too many migration
func (sc *ShardCtrler) assignShardsRange(groups map[int][]string) [NShards]int {
	var shards [NShards]int
	if len(groups) > 0 {
		gids := make([]int, 0)
		for gid := range groups {
			gids = append(gids, gid)
		}
		sort.Ints(gids)

		numshdspergrp := NShards / len(groups)
		grpwithextrashd := NShards % len(groups)
		for i := 0; i < len(gids); i++ {
			start := numshdspergrp*i + sc.min(grpwithextrashd, i)
			length := numshdspergrp + 1
			if i+1 > grpwithextrashd {
				length = numshdspergrp
			}
			for j := start; j < start+length; j++ {
				shards[j] = gids[i]
			}
		}
	}

	return shards
}

func (sc *ShardCtrler) createNewConfig() (Config, Config) {

	cfglen := len(sc.configs)

	var oldcfg Config
	if cfglen > 1 {
		oldcfg = sc.configs[cfglen-1]
	} else {
		oldcfg = Config{}
	}

	newcfg := Config{}
	newcfg.Num = oldcfg.Num + 1
	newcfg.Groups = make(map[int][]string)
	newcfg.Shards = oldcfg.Shards

	return oldcfg, newcfg
}

func (sc *ShardCtrler) assignShardsJoin(joingids []int, shards [NShards]int) [NShards]int {

	for _, ngid := range joingids {

		gidShardsMap := make(map[int][]int)
		for shard, gid := range shards {
			if gid > 0 {
				gidShardsMap[gid] = append(gidShardsMap[gid], shard)
			}
		}

		if len(gidShardsMap) == 0 {
			for i := 0; i < NShards; i++ {
				shards[i] = ngid
			}
			continue
		}

		gidShardsMap[ngid] = make([]int, 0)

		groupCount := len(gidShardsMap)
		if NShards/groupCount < 1 {
			break
		}

		gidShardsArr := createGidShardsArr(gidShardsMap)

		last := groupCount - 1
		first := 0
		for {
			sortGidShardsArray(gidShardsArr)

			maxShardCount := len(gidShardsArr[last].shards)
			minShardCount := len(gidShardsArr[first].shards)
			if maxShardCount-minShardCount > 1 {
				shardid := gidShardsArr[last].shards[0]
				gidShardsArr[last].shards = append(gidShardsArr[last].shards[:0], gidShardsArr[last].shards[1:]...)
				gidShardsArr[0].shards = append(gidShardsArr[0].shards, shardid)
			} else {
				break
			}
		}

		for _, gidshard := range gidShardsArr {
			for _, shardid := range gidshard.shards {
				shards[shardid] = gidshard.gid
			}
		}
	}

	return shards
}

func (sc *ShardCtrler) opJoinExec(args JoinArgs) {

	oldcfg, newcfg := sc.createNewConfig()

	for gid, servers := range oldcfg.Groups {
		newcfg.Groups[gid] = servers
	}

	joingids := make([]int, 0)
	for gid, servers := range args.Servers {
		_, ok := newcfg.Groups[gid]
		if !ok {
			newcfg.Groups[gid] = servers
			joingids = append(joingids, gid)
		}
	}
	sort.Ints(joingids)

	raft.LogPrint(raft.DEBUG, dScServer, "S%d op join new config %+v args servers %+v", sc.me, newcfg, args.Servers)
	//newcfg.Shards = sc.assignShardsRange(newcfg.Groups)
	newcfg.Shards = sc.assignShardsJoin(joingids, newcfg.Shards)
	raft.LogPrint(raft.DEBUG, dScServer, "S%d op join balance new config %+v", sc.me, newcfg)
	sc.configs = append(sc.configs, newcfg)
}

func (sc *ShardCtrler) assignShardsLeave(leavegids []int, shards [NShards]int, remaingids []int) [NShards]int {

	for _, leavegid := range leavegids {

		gidShardsMap := make(map[int][]int)
		for shard, gid := range shards {
			gidShardsMap[gid] = append(gidShardsMap[gid], shard)
		}

		newGroupCount := len(gidShardsMap) - 1
		if newGroupCount == 0 {
			for i := 0; i < NShards; i++ {
				shards[i] = 0
			}
			break
		}

		shardsToAssign := gidShardsMap[leavegid]
		sort.Ints(shardsToAssign)
		shardsPerGroup := len(shardsToAssign) / newGroupCount
		delete(gidShardsMap, leavegid)

		gidShardsArr := createGidShardsArr(gidShardsMap)

		sortGidShardsArray(gidShardsArr)

		if len(remaingids) >= NShards {
			for _, rgid := range remaingids {
				_, ok := gidShardsMap[rgid]
				if !ok {
					var gidshards GidShards
					gidshards.gid = rgid
					gidshards.shards = make([]int, len(shardsToAssign))
					copy(gidshards.shards, shardsToAssign)
					gidShardsArr = append(gidShardsArr, gidshards)
					shardsToAssign = shardsToAssign[:0]
					break
				}
			}
		} else if shardsPerGroup > 0 {
			for i := 0; i < len(gidShardsArr); i++ {
				shardids := make([]int, shardsPerGroup)
				copy(shardids, shardsToAssign[:shardsPerGroup])
				shardsToAssign = append(shardsToAssign[:0], shardsToAssign[shardsPerGroup:]...)
				gidShardsArr[i].shards = append(gidShardsArr[i].shards, shardids...)
			}
		}

		for i, shardid := range shardsToAssign {
			gidShardsArr[i].shards = append(gidShardsArr[i].shards, shardid)
		}

		for _, gidshard := range gidShardsArr {
			for _, shardid := range gidshard.shards {
				shards[shardid] = gidshard.gid
			}
		}
	}
	return shards
}

func (sc *ShardCtrler) opLeaveExec(args LeaveArgs) {

	oldcfg, newcfg := sc.createNewConfig()

	leavegids := make([]int, 0)
	for gid, servers := range oldcfg.Groups {
		find := false
		for _, leavegid := range args.GIDs {
			if gid == leavegid {
				find = true
				break
			}
		}
		if !find {
			newcfg.Groups[gid] = servers
		} else {
			for _, assignedGid := range newcfg.Shards {
				if assignedGid == gid {
					leavegids = append(leavegids, gid)
					break
				}
			}
		}
	}
	sort.Ints(leavegids)

	remaingids := make([]int, 0)
	for gid := range newcfg.Groups {
		remaingids = append(remaingids, gid)
	}
	sort.Ints(remaingids)

	raft.LogPrint(raft.DEBUG, dScServer, "S%d op leave new config %+v args gids %+v", sc.me, newcfg, args.GIDs)
	//newcfg.Shards = sc.assignShardsRange(newcfg.Groups)
	newcfg.Shards = sc.assignShardsLeave(leavegids, newcfg.Shards, remaingids)
	raft.LogPrint(raft.DEBUG, dScServer, "S%d op leave balance new config %+v", sc.me, newcfg)
	sc.configs = append(sc.configs, newcfg)
}

func (sc *ShardCtrler) opMoveExec(args MoveArgs) {

	oldcfg, newcfg := sc.createNewConfig()

	for gid, servers := range oldcfg.Groups {
		newcfg.Groups[gid] = servers
	}
	newcfg.Shards = oldcfg.Shards
	newcfg.Shards[args.Shard] = args.GID

	sc.configs = append(sc.configs, newcfg)
}

func (sc *ShardCtrler) opQueryExec(args QueryArgs) Config {
	config := Config{}
	config.Groups = make(map[int][]string)
	cfgLen := len(sc.configs)
	num := args.Num
	if args.Num < 0 || args.Num > cfgLen {
		num = cfgLen - 1
	}

	config.Num = sc.configs[num].Num
	config.Shards = sc.configs[num].Shards
	for gid, servers := range sc.configs[num].Groups {
		config.Groups[gid] = servers
	}

	return config
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientop = make(map[int64]*OpCache)
	sc.replyChs = make(map[int]chan OpReply)
	sc.lastApplied = 0

	go sc.applier()

	return sc
}
