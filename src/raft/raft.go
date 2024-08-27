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
	"math/rand"
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

	electionTimeout     time.Duration // election timeout, also follower heartbeats timeout
	lastElectionTimeout time.Duration

	electionTime         time.Time
	leaderHeartbeatsTime time.Time

	applyCh chan ApplyMsg

	applyCond *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm  int
	lastSnapshot      []byte

	isNeedApplySnapshot   bool
	isNeedPersistSnapshot bool
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

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
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

	if encoder.Encode(rf.currentTerm) != nil ||
		encoder.Encode(rf.votedFor) != nil ||
		//encoder.Encode(rf.lastApplied) != nil ||
		encoder.Encode(rf.commitIndex) != nil ||
		encoder.Encode(rf.lastIncludedIndex) != nil ||
		encoder.Encode(rf.lastIncludedTerm) != nil ||
		encoder.Encode(rf.log) != nil {
		LogPrint(ERROR, dPersist, "S%d encode error", rf.me)
	}
	/*var err error
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

	err = encoder.Encode(rf.lastIncludedIndex)
	if err != nil {
		Dbg(dPersist, "S%d encode lastIncludedIndex fail, error %s", rf.me, err.Error())
	}

	err = encoder.Encode(rf.lastIncludedTerm)
	if err != nil {
		Dbg(dPersist, "S%d encode lastIncludedTerm fail, error %s", rf.me, err.Error())
	}

	err = encoder.Encode(rf.log)
	if err != nil {
		Dbg(dPersist, "S%d encode log fail, error %s", rf.me, err.Error())
	}*/

	data := byteBuffer.Bytes()
	if len(rf.lastSnapshot) > 0 && rf.isNeedPersistSnapshot {
		rf.persister.SaveStateAndSnapshot(data, rf.lastSnapshot)
		rf.isNeedPersistSnapshot = false
	} else {
		rf.persister.SaveRaftState(data)
	}
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

	var currentTerm int
	var votedFor int
	var commitIndex int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var log []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		//decoder.Decode(&rf.lastApplied) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil ||
		decoder.Decode(&log) != nil {
		LogPrint(ERROR, dPersist, "S%d decode error", rf.me)
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.commitIndex = commitIndex
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.log = make([]LogEntry, len(log))
	copy(rf.log, log)

	rf.lastSnapshot = rf.persister.ReadSnapshot()
	if len(rf.lastSnapshot) > 0 {
		rf.isNeedApplySnapshot = true

	} else {
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = -1
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.applyCond.Broadcast()

	LogPrint(INFO, dPersist, "S%d read raft state lastApplied=%d CI=%d LII=%d LIT=%d log %v",
		rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
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

	if len(snapshot) < 1 {
		LogPrint(ERROR, dSnap, "S%d snapshot byte buffer is nil", rf.me)
		return
	}

	if index > rf.commitIndex || index < rf.lastIncludedIndex {
		LogPrint(ERROR, dSnap, "S%d snapshot index error index=%d CI=%d LII=%d", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	LogPrint(INFO, dSnap, "S%d snapshot LII=%d LIT=%d, log %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)

	rf.lastIncludedTerm = rf.log[rf.logArrIndex(index)].Term

	rf.trimLog(index)
	//rf.log = append(rf.log[:0], rf.log[index+1:]...)

	rf.lastIncludedIndex = index // after trim log
	rf.lastSnapshot = make([]byte, len(snapshot))
	copy(rf.lastSnapshot, snapshot)
	rf.isNeedPersistSnapshot = true

	rf.persist()

	rf.isNeedApplySnapshot = true
	rf.lastApplied = rf.lastIncludedIndex
	rf.applyCond.Broadcast()

	LogPrint(INFO, dSnap, "S%d snapshot LII=%d LIT=%d, trimed log %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
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
		nLLIndex := rf.lastLogIndex()
		LogPrint(INFO, dLog, "S%d Start T=%d ST=%d CI=%d LLI=%d %s",
			rf.me, rf.currentTerm, rf.state, rf.commitIndex, nLLIndex, rf.log[rf.logArrIndex(nLLIndex)].str())
		rf.sendAppendEntries(false)
	} else {
		isLeader = false
	}

	rf.persist()

	index = rf.lastLogIndex()
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
	rf.mu.Lock()
	rf.persist()
	LogPrint(INFO, dInfo, "S%d killed", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applier() {
	for !rf.killed() {

		rf.mu.Lock()

		if rf.isNeedApplySnapshot {
			rf.isNeedApplySnapshot = false
			rf.lastApplied = rf.lastIncludedIndex
			msg := ApplyMsg{false, 0, 0, true, rf.lastSnapshot, rf.lastIncludedTerm, rf.lastIncludedIndex}

			LogPrint(INFO, dClient, "S%d apply snapshot lastApplied=%d CI=%d LII=%d LIT=%d",
				rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)
			rf.mu.Unlock()
			rf.applyCh <- msg // may block, goroutine can not ensure sequence
			rf.mu.Lock()
		} else if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			command := rf.log[rf.logArrIndex(rf.lastApplied)].Command
			msg := ApplyMsg{true, command, rf.lastApplied, false, nil, 0, 0}

			LogPrint(DEBUG, dClient, "S%d apply lastApplied=%d CI=%d LII=%d LIT=%d",
				rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)

			rf.mu.Unlock()
			rf.applyCh <- msg // may block, goroutine can not ensure sequence
			rf.mu.Lock()
		} else {
			LogPrint(INFO, dClient, "S%d apply end lastApplied=%d CI=%d", rf.me, rf.lastApplied, rf.commitIndex)
			LogPrint(DEBUG, dClient, "S%d apply end log %v", rf.me, rf.log)
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetLeaderHeastbeatsTimeout() {
	//LogPrint(DEBUG, dTimer, "S%d reset leader heartbeats timeout", server)
	rf.leaderHeartbeatsTime = time.Now().Add(LeaderHeartbeatsTimeout)
	rf.resetElectionTimeout()
}

func (rf *Raft) sendHeartbeats() {
	if rf.state != LEADER {
		return
	}
	rf.sendAppendEntries(true)
}

func (rf *Raft) tick() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if time.Now().After(rf.electionTime) {
		LogPrint(INFO, dTimer, "S%d timeout, start next election", rf.me)
		rf.startElection()
	}

	if rf.state == LEADER && time.Now().After(rf.leaderHeartbeatsTime) {
		rf.sendHeartbeats()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		time.Sleep(TickInterval)
		LogPrint(DEBUG, dTimer, "S%d tick", rf.me)
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
	LogPrint(INFO, dClient, "S%d make raft", rf.me)
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{-1, nil})

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.isNeedApplySnapshot = false
	rf.isNeedPersistSnapshot = false

	rf.convertToFollower(0)

	rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.setElectionTimeout()
	rf.lastElectionTimeout = rf.electionTimeout
	rf.leaderHeartbeatsTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
