package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (logEntry *LogEntry) str() string {
	return fmt.Sprintf("{T:%d C:%v}", logEntry.Term, logEntry.Command)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int        // LEADER, FOLLOWER or CANDIDATE
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine and term when entry was received by leader (first index is 1)

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	voteCh          chan bool     // channel for reset timeout
	electionTimeout time.Duration // election timeout, also follower heartbeats timeout

	lastElectionTimeout time.Duration

	applyCh chan ApplyMsg

	applierCh chan int

	lastIncludedIndex int
	lastIncludedTerm  int
	lastSnapshot      []byte
	needApplySnapshot bool

	newSnapshot      []byte
	newIncludedIndex int
	newIncludedTerm  int
}

func logStr(log []LogEntry) string {
	str := "["
	for i := 0; i < len(log); i++ {
		// str += fmt.Sprintf("%d ", log[i].Term)
		str += log[i].str() + " "
	}
	if len(log) > 0 {
		str = str[:len(str)-1]
	}
	str += "]"
	return str
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	byteBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(byteBuffer)

	var err error
	err = encoder.Encode(rf.currentTerm)
	if err != nil {
		Dbg(dPersist, "S%d encode currentTerm fail, error %s", rf.me, err.Error())
	}

	err = encoder.Encode(rf.lastApplied)
	if err != nil {
		Dbg(dPersist, "S%d encode lastApplied fail, error %s", rf.me, err.Error())
	}

	err = encoder.Encode(rf.commitIndex)
	if err != nil {
		Dbg(dPersist, "S%d encode commitIndex fail, error %s", rf.me, err.Error())
	}

	err = encoder.Encode(rf.log)
	if err != nil {
		Dbg(dPersist, "S%d encode log fail, error %s", rf.me, err.Error())
	}

	data := byteBuffer.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) decodeOne(decoder *labgob.LabDecoder, key string, value interface{}) bool {

	err := decoder.Decode(value)
	if err != nil {
		Dbg(dPersist, "S%d decode %s fail, error %s", rf.me, key, err.Error())
		return false
	}
	return true
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	rf.mu.Lock()
	defer rf.mu.Unlock()
	byteBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(byteBuffer)
	//var lastApplied int
	//var commitIndex int
	//var log []LogEntry

	var err error
	err = decoder.Decode(&rf.currentTerm)
	if err != nil {
		Dbg(dPersist, "S%d decode currentTerm fail, error %s", rf.me, err.Error())
	}

	err = decoder.Decode(&rf.lastApplied)
	if err != nil {
		Dbg(dPersist, "S%d decode lastApplied fail, error %s", rf.me, err.Error())
	}

	err = decoder.Decode(&rf.commitIndex)
	if err != nil {
		Dbg(dPersist, "S%d decode commitIndex fail, error %s", rf.me, err.Error())
	}

	err = decoder.Decode(&rf.log)
	if err != nil {
		Dbg(dPersist, "S%d decode log fail, error %s", rf.me, err.Error())
	}

	//rf.lastApplied = lastApplied
	//rf.commitIndex = commitIndex
	//copy(rf.log, log) //error, must init rf.log len

	/*if rf.decodeOne(decoder, "lastApplied", &lastApplied) {
		rf.lastApplied = lastApplied
	}

	if rf.decodeOne(decoder, "commitIndex", &commitIndex) {
		rf.commitIndex = commitIndex
	}

	if rf.decodeOne(decoder, "log", &log) {
		copy(rf.log, log)
	}*/

	Dbg(dPersist, "S%d ReadRaftState lastApplied=%d CI=%d log %s", rf.me, rf.lastApplied, rf.commitIndex, logStr(rf.log))
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastIncludedTerm = rf.log[rf.realLogIndex(index)].Term

	Dbg(dSnap, "S%d snapshot last include index %d term %d, log %s", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, logStr(rf.log))

	var log []LogEntry
	log = append(log, rf.log[0])
	trimIndex := rf.realLogIndex(index + 1)
	if trimIndex <= rf.lastLogIndex() {
		log = append(log, rf.log[trimIndex:]...)
	}

	rf.log = log
	//rf.log = append(rf.log[:0], rf.log[index+1:]...)

	Dbg(dSnap, "S%d snapshot last include index %d term %d, trim log %s", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, logStr(rf.log))

	rf.lastIncludedIndex = index
	rf.lastSnapshot = make([]byte, len(snapshot))
	copy(rf.lastSnapshot, snapshot)
	rf.needApplySnapshot = true
	rf.notifyApply(rf.lastIncludedIndex)

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

type RequestAppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogTerm  int        // term of prevLogIndex entry
	PrevLogIndex int        // index of log entry immediately preceding new ones
	LeaderCommit int        // leader’s commitIndex
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

func (reqAppendArgs *RequestAppendEntriesArgs) str() string {
	return fmt.Sprintf("[T=%d PLT=%d PLI=%d CI=%d]",
		reqAppendArgs.Term, reqAppendArgs.PrevLogTerm, reqAppendArgs.PrevLogIndex, reqAppendArgs.LeaderCommit)
}

type RequestAppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // the protocol can be optimized to reduce the number of rejected AppendEntries RPCs
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	//Dbg(dLog, "S%d RequestAppendEntries from S%d T%d", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Dbg(dLog, "S%d [T=%d LLI=%d LLT=%d ST=%d CI=%d LII=%d LIT=%d] receive append entries from S%d %s HR=%d",
		rf.me, rf.currentTerm, rf.lastLogIndex(), rf.log[rf.lastLogIndex()].Term, rf.state,
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

	realLLIndex := rf.lastLogIndex() + rf.lastIncludedIndex
	prevLogIndex := rf.realLogIndex(args.PrevLogIndex)
	if prevLogIndex > rf.lastLogIndex() {
		prevLogIndex = rf.lastLogIndex()
	}
	prevLogTerm := -1
	if args.PrevLogIndex == rf.lastIncludedIndex {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	if realLLIndex < args.PrevLogIndex || prevLogTerm != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if realLLIndex < args.PrevLogIndex {
			reply.ConflictIndex = realLLIndex + 1
		} else {
			index := args.PrevLogIndex - 1
			for {
				Dbg(dLog, "S%d index %d lastIncludedIndex %d", rf.me, index, rf.lastIncludedIndex)
				if prevLogTerm != rf.log[rf.realLogIndex(index)].Term {
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
		for i := 0; i < len(args.Entries) && index <= realLLIndex; i++ {
			realIndex := rf.realLogIndex(index)
			if rf.log[realIndex].Term != args.Entries[i].Term ||
				(rf.log[realIndex].Term == args.Entries[i].Term &&
					!reflect.DeepEqual(rf.log[realIndex].Command, args.Entries[i].Command)) { // compare interface ?
				rf.log = rf.log[:realIndex]
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
		prevCommitIndex := rf.commitIndex
		if args.LeaderCommit < rf.lastLogIndex()+rf.lastIncludedIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex() + rf.lastIncludedIndex
		}
		rf.notifyApply(prevCommitIndex)
	}
	/*for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = rf.lastLogIndex()
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}*/
	reply.Success = true

}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		isLeader = true
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		Dbg(dLog, "S%d Start T=%d ST=%d CI=%d LLI=%d %s",
			rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastLogIndex(), rf.log[rf.lastLogIndex()].str())
		rf.sendAppendEntries(false)
	} else {
		isLeader = false
	}

	rf.persist()

	index = rf.lastLogIndex() + rf.lastIncludedIndex
	term = rf.currentTerm

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) lastLogIndex() int {
	return (len(rf.log) - 1)
}

func (rf *Raft) realLogIndex(index int) int {
	if index >= rf.lastIncludedIndex {
		return index - rf.lastIncludedIndex
	}

	return 0
}

func (rf *Raft) resetLeaderHeastbeatsTimeout(server int) {
	//Dbg(dTimer, "S%d reset leader heartbeats timeout", server)
	rf.resetElectionTimeout(server)
}

func (rf *Raft) notifyApply(startCommitIndex int) {
	go func() {
		Dbg(dLog, "S%d notify apply", rf.me)
		rf.applierCh <- startCommitIndex
	}()
}

func (rf *Raft) applier() {
	for !rf.killed() {
		select {
		case <-rf.applierCh:
			rf.mu.Lock()
			//defer rf.mu.Unlock()

			if rf.needApplySnapshot {
				msg := ApplyMsg{false, 0, 0, true, rf.lastSnapshot, rf.lastIncludedTerm, rf.lastIncludedIndex}
				Dbg(dSnap, "S%d apply message LII=%d LIT=%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
				rf.needApplySnapshot = false
				rf.lastApplied = rf.lastIncludedIndex
				rf.mu.Unlock()
				rf.applyCh <- msg // may block, goroutine can not ensure sequence
				rf.mu.Lock()
			} else {
				Dbg(dPersist, "S%d apply start lastApplied %d CI %d", rf.me, rf.lastApplied, rf.commitIndex)
				if rf.commitIndex > rf.lastApplied {
					for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
						rf.lastApplied = index
						Dbg(dPersist, "S%d apply message lastApplied=%d CI=%d", rf.me, rf.lastApplied, rf.commitIndex)
						msg := ApplyMsg{true, rf.log[rf.realLogIndex(index)].Command, index, false, nil, 0, 0}
						rf.mu.Unlock()
						rf.applyCh <- msg // may block, goroutine can not ensure sequence
						rf.mu.Lock()
					}
					rf.persist()
					Dbg(dPersist, "S%d apply message lastApplied=%d CI=%d log %s", rf.me, rf.lastApplied, rf.commitIndex, logStr(rf.log))
				}
			}

			rf.mu.Unlock()
		case <-time.After(20 * time.Millisecond):
			//Dbg(dPersist, "S%d apply start index timeout", rf.me)
			continue
		}

		/*select {
		case startCommitIndex := <-rf.applierCh:
			Dbg(dPersist, "S%d apply start index %d", rf.me, startCommitIndex)
			rf.mu.Lock()
			//defer rf.mu.Unlock()
			for index := startCommitIndex; index <= rf.commitIndex; index++ {
				if index > rf.lastApplied {
					rf.lastApplied = index
					rf.persist()
					rf.applyCh <- ApplyMsg{true, rf.log[index].Command, index, false, nil, 0, 0}
				}
			}
			rf.mu.Unlock()
		case <-time.After(50 * time.Millisecond):
			//Dbg(dPersist, "S%d apply start index timeout", rf.me)
			continue
		}*/
	}
}

func (rf *Raft) prepareAppendEntriesArgs(peer int, heartbeats bool) *RequestAppendEntriesArgs {

	myLLIndex := rf.lastLogIndex()
	//prevLogIndex := rf.matchIndex[peer]

	prevLogIndex := 0
	prevLogTerm := -1
	nextIndex := 1
	if rf.nextIndex[peer] > rf.lastIncludedIndex {
		prevLogIndex = rf.nextIndex[peer] - 1
	} else {
		if rf.nextIndex[peer] > myLLIndex {
			rf.nextIndex[peer] = myLLIndex + 1
		}
		prevLogIndex = rf.nextIndex[peer] - 1 + rf.lastIncludedIndex
	}

	if prevLogIndex == rf.lastIncludedIndex {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		prevLogTerm = rf.log[rf.realLogIndex(prevLogIndex)].Term
	}

	nextIndex = rf.realLogIndex(prevLogIndex + 1)

	args := &RequestAppendEntriesArgs{rf.currentTerm, rf.me, prevLogTerm, prevLogIndex, rf.commitIndex, nil}

	if myLLIndex >= nextIndex {
		args.Entries = make([]LogEntry, len(rf.log)-nextIndex)
		copy(args.Entries, rf.log[nextIndex:])
	}
	Dbg(dLeader, "S%d send append entries to S%d [T=%d LLI=%d LLT=%d PLI=%d PLT=%d NI=%d MI=%d CI=%d LII=%d HR=%t]",
		rf.me, peer, args.Term, myLLIndex, rf.log[myLLIndex].Term, args.PrevLogIndex, args.PrevLogTerm,
		nextIndex, rf.matchIndex[peer], rf.commitIndex, rf.lastIncludedIndex, heartbeats)

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

		if count > len(rf.peers)/2 && rf.log[rf.realLogIndex(minIndex)].Term == rf.currentTerm {
			rf.commitIndex = minIndex

			Dbg(dLeader, "S%d receive append entries reply from S%d, majority [T=%d MI=%d CI=%d]",
				rf.me, peer, args.Term, rf.matchIndex[rf.me], rf.commitIndex)

			rf.notifyApply(rf.matchIndex[rf.me])
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
		}
	}

	if rf.state == LEADER && rf.nextIndex[peer] <= rf.lastIncludedIndex {
		//rf.sendInstallSnapshot(peer)
	}
}

func (rf *Raft) sendAppendEntries(heartbeats bool) {

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {

			args := rf.prepareAppendEntriesArgs(peer, heartbeats)

			go func(server int, args *RequestAppendEntriesArgs) {
				var reply RequestAppendEntriesReply

				//Dbg(dLeader, "S%d append entries to S%d T%d, heartbeats %t", rf.me, server, args.Term, heartbeats)
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

func (rf *Raft) sendHeartbeats() {
	if rf.state != LEADER {
		return
	}
	rf.sendAppendEntries(true)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.doElection()

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == LEADER {
			time.Sleep(LeaderHeartbeatsTimeout)
			rf.mu.Lock()
			rf.sendHeartbeats()
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	Dbg(dClient, "S%d make raft", rf.me)
	rf.applyCh = applyCh

	rf.convertToFollower(0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, LogEntry{-1, nil})
	//rf.log = append(rf.log, LogEntry{rf.currentTerm, nil})
	rf.voteCh = make(chan bool)
	rf.applierCh = make(chan int)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	rf.needApplySnapshot = false

	rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.setElectionTimeout()
	rf.lastElectionTimeout = rf.electionTimeout

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
