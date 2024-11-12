package raft

import (
	"fmt"
	"reflect"
)

type RequestAppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogTerm  int        // term of prevLogIndex entry
	PrevLogIndex int        // index of log entry immediately preceding new ones
	LeaderCommit int        // leader’s commitIndex
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

func (args *RequestAppendEntriesArgs) str() string {
	return fmt.Sprintf("args:[T=%d PLI=%d PLT=%d CI=%d Len=%d]",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
}

type RequestAppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // the protocol can be optimized to reduce the number of rejected AppendEntries RPCs
	ConflictTerm  int
}

func (reply *RequestAppendEntriesReply) str() string {
	return fmt.Sprintf("reply:[T=%d CONI=%d CONT=%d SUCC=%t]",
		reply.Term, reply.ConflictIndex, reply.ConflictTerm, reply.Success)
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) searchConflictIndex2(endIndex int, targetTerm int) int {
	index := 0
	lowIndex := 1
	highIndex := rf.logArrIndex(endIndex)

	for lowIndex <= highIndex {
		midIndex := (lowIndex + highIndex) / 2
		term := rf.logEntryTerm(midIndex + rf.lastIncludedIndex)
		LogPrint(DEBUG, dLog, "S%d search conflict index [I=%d T=%d PLI=%d PLT=%d LII=%d LLT=%d]",
			rf.me, midIndex, term, endIndex, targetTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)

		if term == targetTerm {
			index = midIndex
			highIndex = midIndex - 1
		} else if term < targetTerm {
			lowIndex = midIndex + 1
		} else {
			highIndex = midIndex - 1
		}
	}

	return index + rf.lastIncludedIndex
}

func (rf *Raft) searchConflictIndex1(prevLogIndex int) int {
	index := prevLogIndex - 1
	for {
		// index <= 1, be careful endless loop
		if rf.logEntryTerm(prevLogIndex) != rf.logEntryTerm(index) || index <= LogStartIndex {
			LogPrint(INFO, dLog, "S%d search conflict index [I=%d T=%d PLI=%d PLT=%d LII=%d LLT=%d]",
				rf.me, index, rf.logEntryTerm(index), prevLogIndex, rf.logEntryTerm(prevLogIndex), rf.lastIncludedIndex, rf.lastIncludedTerm)
			break
		}
		index--
	}
	return index + 1
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	LogPrint(INFO, dLog, "S%d [T=%d LLI=%d LLT=%d ST=%d CI=%d LII=%d LIT=%d] recv append entries req from S%d %s HR=%d",
		rf.me, rf.currentTerm, rf.lastLogIndex(), rf.log[rf.logArrIndex(rf.lastLogIndex())].Term, rf.state,
		rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LeaderId, args.str(), len(args.Entries))

	nCurrentTerm := rf.currentTerm

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > nCurrentTerm { // leader term > my term => follower
		rf.convertToFollower(args.Term)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < nCurrentTerm { // leader term < my term, reject
		reply.Term = rf.currentTerm
		reply.Success = false
		return // if the term in the AppendEntries arguments is outdated, you should not reset your timer
	}

	rf.resetElectionTimeout()

	// in lab 3, create snapshot by log size, so each server's lastIncludedIndex may different
	// when args.PrevLogIndex < rf.lastIncludedIndex need reply false
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	isNeedPersist := false
	lenEntries := len(args.Entries)

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// last log index < PrevLogIndex, same
	nLLIndex := rf.lastLogIndex()
	if nLLIndex < args.PrevLogIndex || rf.logEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if nLLIndex < args.PrevLogIndex {
			reply.ConflictIndex = nLLIndex + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictIndex = rf.searchConflictIndex1(args.PrevLogIndex)
			reply.ConflictTerm = rf.logEntryTerm(args.PrevLogIndex)
		}
		return
	} else if lenEntries > 0 {
		//LogPrint(INFO, dLog, "S%d log size=%d log %s entries %s", rf.me, logEntryByteSize(rf.log), logStr(rf.log), logStr(args.Entries))

		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)
		index := args.PrevLogIndex + 1
		for i := 0; i < lenEntries && index <= nLLIndex; i++ {
			arrIndex := rf.logArrIndex(index)
			if rf.log[arrIndex].Term != args.Entries[i].Term ||
				(rf.log[arrIndex].Term == args.Entries[i].Term &&
					!reflect.DeepEqual(rf.log[arrIndex].Command, args.Entries[i].Command)) { // compare interface ?
				rf.log = rf.log[:arrIndex]
				break
			}
			index += 1
		}
		conflictIndex := index - (args.PrevLogIndex + 1)
		LogPrint(INFO, dLog, "S%d index=%d conflictIndex=%d log size=%d", rf.me, index, conflictIndex, logEntryByteSize(rf.log))
		// 4. Append any new entries not already in the log
		if conflictIndex < lenEntries {
			LogPrint(DEBUG, dLog, "S%d log %v entries %v", rf.me, rf.log, args.Entries) // logStr cost time result to TestSpeed3A failed
			rf.log = append(rf.log, args.Entries[conflictIndex:]...)
			isNeedPersist = true
		}

		//LogPrint(INFO, dLog, "S%d log size %d log %s", rf.me, logEntryByteSize(rf.log), logStr(rf.log))
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		isNeedPersist = true
	}

	if isNeedPersist {
		rf.persist()
	}

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	if rf.commitIndex > rf.lastApplied {
		rf.applyCond.Signal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	LogPrint(INFO, dLog, "S%d [T=%d LLI=%d LLT=%d ST=%d CI=%d LII=%d LIT=%d] send append entries response to S%d",
		rf.me, rf.currentTerm, rf.lastLogIndex(), rf.log[rf.logArrIndex(rf.lastLogIndex())].Term, rf.state,
		rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LeaderId)
}

func (rf *Raft) prepareAppendEntriesArgs(peer int, heartbeats bool) *RequestAppendEntriesArgs {

	//prevLogIndex := rf.matchIndex[peer]

	// rf.nextIndex[peer] >= 1
	if rf.nextIndex[peer] <= rf.lastIncludedIndex {
		rf.nextIndex[peer] = rf.lastIncludedIndex + 1
	}

	/*if rf.nextIndex[peer] > LogStartIndex { // conflict index may = 1, prevLogIndex >= LogStartIndex
		prevLogIndex := rf.nextIndex[peer] - 1
	}*/

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.logEntryTerm(prevLogIndex)

	nLLArrIndex := rf.logArrIndex(rf.lastLogIndex())
	nextIndex := rf.logArrIndex(rf.nextIndex[peer])

	args := &RequestAppendEntriesArgs{rf.currentTerm, rf.me, prevLogTerm, prevLogIndex, rf.commitIndex, nil}

	if nLLArrIndex >= nextIndex {
		args.Entries = make([]LogEntry, len(rf.log)-nextIndex)
		copy(args.Entries, rf.log[nextIndex:])
	}

	LogPrint(INFO, dLeader, "S%d [LLI=%d LLT=%d NI=%d MI=%d LII=%d LIT=%d] send append entries to S%d %s HR=%t",
		rf.me, rf.lastLogIndex(), rf.log[nLLArrIndex].Term, rf.nextIndex[peer], rf.matchIndex[peer],
		rf.lastIncludedIndex, rf.lastIncludedTerm, peer, args.str(), heartbeats)

	return args
}

func (rf *Raft) processAppendEntriesReply(peer int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LogPrint(INFO, dLeader, "S%d T=%d %s recv append entries res from S%d %s", rf.me, rf.currentTerm, args.str(), peer, reply.str())

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	if reply.Success {

		if args.Term == rf.currentTerm {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			if matchIndex > rf.matchIndex[peer] { // response reorder
				rf.matchIndex[peer] = matchIndex
			}

			nextIndex := matchIndex + 1
			if nextIndex > rf.nextIndex[peer] { // response reorder
				rf.nextIndex[peer] = nextIndex
			}

			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
			nCount := 1
			nIndex := rf.matchIndex[peer]
			for i := 0; i < len(rf.matchIndex); i++ {
				if i != rf.me && rf.matchIndex[i] > rf.commitIndex {
					nCount++
					if rf.matchIndex[i] < nIndex { // the min index for commit
						nIndex = rf.matchIndex[i]
					}
				}
			}

			nIndexTerm := rf.log[rf.logArrIndex(nIndex)].Term

			LogPrint(INFO, dLeader, "S%d [T=%d MI=%d NI=%d CI=%d LII=%d LIT=%d CNT=%d N=%d NT=%d] recv append entries res from S%d",
				rf.me, args.Term, rf.matchIndex[peer], rf.nextIndex[peer], rf.commitIndex,
				rf.lastIncludedIndex, rf.lastIncludedTerm, nCount, nIndex, nIndexTerm, peer)

			if nCount > len(rf.peers)/2 && nIndexTerm == rf.currentTerm {
				rf.commitIndex = nIndex

				LogPrint(INFO, dLeader, "S%d recv append entries res from S%d, majority [T=%d CI=%d]",
					rf.me, peer, args.Term, rf.commitIndex)

				// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
				// the leader applies the entry to its state machine
				rf.persist()
				//rf.sendHeartbeats() // improve execute time, is it need ?

				//rf.matchIndex[rf.me] = rf.commitIndex
				//rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
			}
		}
	} else {
		LogPrint(INFO, dLeader, "S%d [T=%d ST=%d] args:[T=%d] recv append entries res from S%d",
			rf.me, rf.currentTerm, rf.state, args.Term, peer)
		// in lab 3, create snapshot by log size, so each server's lastIncludedIndex may different
		// when args.PrevLogIndex < rf.lastIncludedIndex need reply false
		// Leader may always send same PrevLogIndex to Follower, endless loop then log can not commit
		if args.Term == rf.currentTerm &&
			(reply.ConflictIndex < rf.nextIndex[peer] || // response reorder
				(rf.lastIncludedIndex > 0 && rf.lastIncludedIndex < reply.ConflictIndex)) { // todo
			rf.nextIndex[peer] = reply.ConflictIndex

			//rf.nextIndex[peer] -= 1
			//rf.matchIndex[peer] -= 1

			// the leader must occasionally send snapshots to followers that lag behind. This happens when the leader
			// has already discarded the next log entry that it needs to send to a follower.
			if rf.state == LEADER && rf.nextIndex[peer] <= rf.lastIncludedIndex {
				rf.sendInstallSnapshot(peer)
			}
		}
		/*if args.Term == rf.currentTerm {
			nextIndex := 1
			if reply.ConflictTerm == -1 {
				nextIndex = reply.ConflictIndex
			} else {
				conflictIndex := -1
				for i := args.PrevLogIndex; i > 0; i-- {
					if rf.logEntryTerm(i) == reply.ConflictTerm {
						conflictIndex = i
						break
					}
				}

				if conflictIndex != -1 {
					nextIndex = conflictIndex + 1
				} else {
					nextIndex = reply.ConflictIndex
				}
			}

			if nextIndex < rf.nextIndex[peer] { // reponse reorder
				rf.nextIndex[peer] = nextIndex
			}

			//rf.nextIndex[peer] -= 1
			//rf.matchIndex[peer] -= 1
			if rf.state == LEADER && rf.nextIndex[peer] <= rf.lastIncludedIndex {
				rf.sendInstallSnapshot(peer)
			}
		}*/
	}

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	// the leader returns the result of that execution to the client
	if rf.commitIndex > rf.lastApplied {
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(heartbeats bool) {

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {

			args := rf.prepareAppendEntriesArgs(peer, heartbeats)

			go func(server int, args *RequestAppendEntriesArgs) {
				var reply RequestAppendEntriesReply
				ok := rf.sendRequestAppendEntries(server, args, &reply)

				if ok {
					rf.processAppendEntriesReply(server, args, &reply)
				}
			}(peer, args)
		} else {
			if heartbeats {
				rf.resetLeaderHeastbeatsTimeout()
			}
		}
	}
}
