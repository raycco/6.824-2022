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
}

func (reply *RequestAppendEntriesReply) str() string {
	return fmt.Sprintf("reply:[T=%d CONI=%d SUCC=%t]",
		reply.Term, reply.ConflictIndex, reply.Success)
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
		if rf.logEntryTerm(prevLogIndex) != rf.logEntryTerm(index) || index <= LogStartIndex {
			LogPrint(INFO, dLog, "S%d search conflict index [I=%d T=%d PLI=%d PLT=%d LII=%d LLT=%d]",
				rf.me, index, rf.logEntryTerm(index), prevLogIndex, rf.logEntryTerm(prevLogIndex), rf.lastIncludedIndex, rf.lastIncludedTerm)
			break
		}
		LogPrint(DEBUG, dLog, "S%d search conflict index [I=%d T=%d PLI=%d PLT=%d LII=%d LLT=%d]",
			rf.me, index, rf.logEntryTerm(index), prevLogIndex, rf.logEntryTerm(prevLogIndex), rf.lastIncludedIndex, rf.lastIncludedTerm)
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

	nTerm := rf.currentTerm

	if args.Term > nTerm { // leader term > my term => follower
		rf.convertToFollower(args.Term)
	}

	if args.Term < nTerm { // leader term < my term, reject
		reply.Term = rf.currentTerm
		reply.Success = false
		return // if the term in the AppendEntries arguments is outdated, you should not reset your timer
	}

	rf.resetElectionTimeout(rf.me)

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	isNeedPersist := false
	lenEntries := len(args.Entries)

	nLLIndex := rf.lastLogIndex()
	if nLLIndex < args.PrevLogIndex || rf.logEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if nLLIndex < args.PrevLogIndex {
			reply.ConflictIndex = nLLIndex + 1
		} else {
			if nLLIndex < 100 {
				reply.ConflictIndex = rf.searchConflictIndex1(args.PrevLogIndex)
			} else {
				reply.ConflictIndex = rf.searchConflictIndex2(args.PrevLogIndex, rf.logEntryTerm(args.PrevLogIndex))
			}
		}
		return
	} else if lenEntries > 0 {
		//LogPrint(INFO, dLog, "S%d log size=%d log %s entries %s", rf.me, logEntryByteSize(rf.log), logStr(rf.log), logStr(args.Entries))

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
		if conflictIndex < lenEntries {
			LogPrint(DEBUG, dLog, "S%d log %v entries %v", rf.me, rf.log, args.Entries) // logStr cost time result to TestSpeed3A failed
			rf.log = append(rf.log, args.Entries[conflictIndex:]...)
			isNeedPersist = true
		}

		//LogPrint(INFO, dLog, "S%d log size %d log %s", rf.me, logEntryByteSize(rf.log), logStr(rf.log))
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.notifyApply()
		isNeedPersist = true
	}

	if isNeedPersist {
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	LogPrint(INFO, dLog, "S%d [T=%d LLI=%d LLT=%d ST=%d CI=%d LII=%d LIT=%d] send append entries response to S%d",
		rf.me, rf.currentTerm, rf.lastLogIndex(), rf.log[rf.logArrIndex(rf.lastLogIndex())].Term, rf.state,
		rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LeaderId)
}

func (rf *Raft) prepareAppendEntriesArgs(peer int, heartbeats bool) *RequestAppendEntriesArgs {

	//prevLogIndex := rf.matchIndex[peer]

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

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}

	if reply.Success {

		if args.Term == rf.currentTerm {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			if matchIndex > rf.matchIndex[peer] { // respone reorder
				rf.matchIndex[peer] = matchIndex
			}

			nextIndex := matchIndex + 1
			if nextIndex > rf.nextIndex[peer] { // respone reorder
				rf.nextIndex[peer] = nextIndex
			}

			LogPrint(INFO, dLeader, "S%d [T=%d MI=%d NI=%d CI=%d LII=%d LIT=%d] recv append entries res from S%d",
				rf.me, args.Term, rf.matchIndex[peer], rf.nextIndex[peer], rf.commitIndex,
				rf.lastIncludedIndex, rf.lastIncludedTerm, peer)
		}

		count := 1
		minIndex := rf.matchIndex[peer]
		for i := 0; i < len(rf.matchIndex); i++ {
			if i != rf.me && rf.matchIndex[i] > rf.commitIndex {
				count++
				if rf.matchIndex[i] < minIndex { // the min index for commit
					minIndex = rf.matchIndex[i]
				}
			}
		}

		if count > len(rf.peers)/2 && rf.log[rf.logArrIndex(minIndex)].Term == rf.currentTerm {
			rf.commitIndex = minIndex

			LogPrint(INFO, dLeader, "S%d recv append entries res from S%d, majority [T=%d CI=%d]",
				rf.me, peer, args.Term, rf.commitIndex)

			rf.notifyApply()
			rf.persist()
			//rf.matchIndex[rf.me] = rf.commitIndex
			//rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
		}
	} else {
		LogPrint(INFO, dLeader, "S%d [T=%d ST=%d] args:[T=%d] recv append entries res from S%d",
			rf.me, rf.currentTerm, rf.state, args.Term, peer)
		if args.Term == rf.currentTerm &&
			reply.ConflictIndex < rf.nextIndex[peer] { // reponse reorder
			rf.nextIndex[peer] = reply.ConflictIndex
			//rf.nextIndex[peer] -= 1
			//rf.matchIndex[peer] -= 1
			if rf.state == LEADER && rf.nextIndex[peer] <= rf.lastIncludedIndex {
				rf.sendInstallSnapshot(peer)
			}
		}
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
				rf.resetLeaderHeastbeatsTimeout(peer)
			}
		}
	}
}
