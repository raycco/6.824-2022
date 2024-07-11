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

func (reqAppendArgs *RequestAppendEntriesArgs) str() string {
	return fmt.Sprintf("[T=%d PLI=%d PLT=%d CI=%d]",
		reqAppendArgs.Term, reqAppendArgs.PrevLogTerm, reqAppendArgs.PrevLogIndex, reqAppendArgs.LeaderCommit)
}

type RequestAppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // the protocol can be optimized to reduce the number of rejected AppendEntries RPCs
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	//Dbg(dLog, "S%d RequestAppendEntries from S%d T%d", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Dbg(dLog, "S%d [T=%d LLI=%d LLT=%d ST=%d CI=%d LII=%d LIT=%d] receive append entries from S%d %s HR=%d",
		rf.me, rf.currentTerm, rf.lastLogIndex(), rf.log[rf.logArrIndex(rf.lastLogIndex())].Term, rf.state,
		rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LeaderId, args.str(), len(args.Entries))

	myTerm := rf.currentTerm

	if args.Term > myTerm { // leader term > my term => follower
		rf.convertToFollower(args.Term)
	}

	if args.Term < myTerm { // leader term < my term, reject
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

	nLLIndex := rf.lastLogIndex()
	prevLogIndex := args.PrevLogIndex
	if args.PrevLogIndex > nLLIndex {
		prevLogIndex = nLLIndex
	}
	prevLogTerm := rf.logEntryTerm(prevLogIndex)
	if nLLIndex < args.PrevLogIndex || prevLogTerm != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if nLLIndex < args.PrevLogIndex {
			reply.ConflictIndex = nLLIndex + 1
		} else {
			index := args.PrevLogIndex - 1
			for {
				Dbg(dLog, "S%d search conflict index [I=%d T=%d PLI=%d PLT=%d LII=%d LLT=%d]",
					rf.me, index, rf.logEntryTerm(index), prevLogIndex, prevLogTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)
				if prevLogTerm != rf.logEntryTerm(index) || index == 0 {
					break
				}
				index--
			}
			reply.ConflictIndex = index + 1
		}
		return
	} else if len(args.Entries) > 0 {
		if logEntryByteSize(rf.log) < 4096 && logEntryByteSize(args.Entries) < 4096 {
			Dbg(dLog, "S%d log size %d log %s entries %s", rf.me, logEntryByteSize(rf.log), logStr(rf.log), logStr(args.Entries))
		} else {
			Dbg(dLog, "S%d log size %d entries size %d", rf.me, logEntryByteSize(rf.log), logEntryByteSize(args.Entries))
		}

		index := args.PrevLogIndex + 1
		for i := 0; i < len(args.Entries) && index <= nLLIndex; i++ {
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
		rf.log = append(rf.log, args.Entries[conflictIndex:]...)
		rf.persist()

		if logEntryByteSize(rf.log) < 4096 {
			Dbg(dLog, "S%d log size %d log %s", rf.me, logEntryByteSize(rf.log), logStr(rf.log))
		} else {
			Dbg(dLog, "S%d log size %d", rf.me, logEntryByteSize(rf.log))
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.notifyApply()
	}
	/*for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = rf.lastLogIndex()
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}*/
	reply.Success = true

}

func (rf *Raft) prepareAppendEntriesArgs(peer int, heartbeats bool) *RequestAppendEntriesArgs {

	nLLIndex := rf.logArrIndex(rf.lastLogIndex())
	//prevLogIndex := rf.matchIndex[peer]

	if rf.nextIndex[peer] <= rf.lastIncludedIndex {
		rf.nextIndex[peer] = rf.lastIncludedIndex + 1
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.logEntryTerm(prevLogIndex)
	nextIndex := rf.logArrIndex(rf.nextIndex[peer])

	args := &RequestAppendEntriesArgs{rf.currentTerm, rf.me, prevLogTerm, prevLogIndex, rf.commitIndex, nil}

	if nLLIndex >= nextIndex {
		args.Entries = make([]LogEntry, len(rf.log)-nextIndex)
		copy(args.Entries, rf.log[nextIndex:])
	}
	Dbg(dLeader, "S%d send append entries to S%d [T=%d LLI=%d LLT=%d PLI=%d PLT=%d NI=%d MI=%d CI=%d LII=%d LIT=%d HR=%t]",
		rf.me, peer, args.Term, nLLIndex, rf.log[nLLIndex].Term, args.PrevLogIndex, args.PrevLogTerm,
		rf.nextIndex[peer], rf.matchIndex[peer], rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, heartbeats)

	return args
}

func (rf *Raft) processAppendEntriesReply(peer int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}

	if reply.Success {

		if args.Term == rf.currentTerm {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			if matchIndex > rf.matchIndex[peer] {
				rf.matchIndex[peer] = matchIndex
			}

			nextIndex := matchIndex + 1
			if nextIndex > rf.nextIndex[peer] {
				rf.nextIndex[peer] = nextIndex
			}

			Dbg(dLeader, "S%d [T=%d MI=%d NI=%d CI=%d LII=%d] receive append entries reply from S%d",
				rf.me, args.Term, rf.matchIndex[peer], rf.nextIndex[peer], rf.commitIndex, rf.lastIncludedIndex, peer)
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

			Dbg(dLeader, "S%d receive append entries reply from S%d, majority [T=%d MI=%d CI=%d]",
				rf.me, peer, args.Term, rf.matchIndex[rf.me], rf.commitIndex)

			rf.notifyApply()
			//rf.matchIndex[rf.me] = rf.commitIndex
			//rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
		}
	} else {
		Dbg(dLeader, "S%d [T=%d ST=%d ArgsT=%d] receive append entries reply from S%d [T=%d ConflictIndex=%d]",
			rf.me, rf.currentTerm, rf.state, args.Term, peer, reply.Term, reply.ConflictIndex)
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
