package raft

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedTerm  int    // term of lastIncludedIndex
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendRequestInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	myTerm := rf.currentTerm

	if args.Term > myTerm { // leader term > my term => follower
		rf.convertToFollower(args.Term)
	}

	if args.Term < myTerm { // leader term < my term, reject
		reply.Term = rf.currentTerm
		return // if the term in the AppendEntries arguments is outdated, you should not reset your timer
	}

	if args.Offset == 0 {
		if rf.lastIncludedIndex < args.LastIncludedIndex {
			Dbg(dSnap, "S%d snapshot last include index %d term %d, log %s", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, logStr(rf.log))
			var log []LogEntry
			log = append(log, rf.log[0])
			trimIndex := rf.realLogIndex(args.LastIncludedIndex + 1)
			if trimIndex <= rf.lastLogIndex() {
				log = append(log, rf.log[trimIndex:]...)
			}
			rf.log = log
			Dbg(dSnap, "S%d snapshot last include index %d term %d, trim log %s", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, logStr(rf.log))

			rf.lastSnapshot = append(rf.lastSnapshot[:0], args.Data...)
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.needApplySnapshot = true

			rf.persist()
			rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
			rf.notifyApply(rf.lastIncludedIndex)
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
