package raft

import "fmt"

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedTerm  int    // term of lastIncludedIndex
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

func (args *InstallSnapshotArgs) str() string {
	return fmt.Sprintf("args:[T=%d LII=%d LIT=%d DataLen=%d]",
		args.Term, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendRequestInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) trimLog(index int) {
	var log []LogEntry
	log = append(log, rf.log[0])
	trimIndex := rf.logArrIndex(index + 1)
	if trimIndex < len(rf.log) {
		log = append(log, rf.log[trimIndex:]...)
	}
	rf.log = log
}

func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nTerm := rf.currentTerm

	if args.Term > nTerm { // leader term > my term => follower
		rf.convertToFollower(args.Term)
	}

	if args.Term < nTerm { // leader term < my term, reject
		reply.Term = rf.currentTerm
		return // if the term in the AppendEntries arguments is outdated, you should not reset your timer
	}

	if args.Offset == 0 {
		if rf.lastIncludedIndex < args.LastIncludedIndex {
			LogPrint(INFO, dSnap, "S%d [LII=%d LIT=%d] recv snapshot req from S%d %v, log %s",
				rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LeaderId, args.str(), rf.log)

			rf.trimLog(args.LastIncludedIndex)
			rf.lastSnapshot = make([]byte, len(args.Data))
			copy(rf.lastSnapshot, args.Data)
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.isNeedPersistSnapshot = true

			rf.lastApplied = rf.lastIncludedIndex
			if rf.commitIndex < rf.lastIncludedIndex {
				rf.commitIndex = rf.lastIncludedIndex
			}

			LogPrint(INFO, dSnap, "S%d [LII=%d LIT=%d] recv snapshot req from S%d %s, trimed log %v",
				rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LeaderId, args.str(), rf.log)

			rf.persist()

			rf.isNeedApplySnapshot = true
			rf.notifyApply()
		} else {
			if rf.lastIncludedTerm == args.LastIncludedTerm {

			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	if len(rf.lastSnapshot) > 0 {
		var args InstallSnapshotArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LastIncludedTerm = rf.lastIncludedTerm
		args.LastIncludedIndex = rf.lastIncludedIndex
		args.Offset = 0
		args.Data = make([]byte, len(rf.lastSnapshot))
		copy(args.Data, rf.lastSnapshot)
		args.Done = true

		go func(peer int, args *InstallSnapshotArgs) {
			LogPrint(INFO, dSnap, "S%d %s send snapshot req to S%d", rf.me, args.str(), peer)

			var reply InstallSnapshotReply
			ok := rf.sendRequestInstallSnapshot(peer, args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
				}
			}
		}(peer, &args)
	}
}
