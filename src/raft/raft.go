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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  []uint64
	matchIndex []uint64

	voteCh       chan bool
	electionTime time.Duration
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

}

type RequestAppendEntryArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit uint64
	Entries      []LogEntry
}

type RequestAppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Entries == nil {
		Dbg(dTimer, "S%d receive heartbeats from S%d T%d, my T%d state %d, channel %d",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state, len(rf.voteCh))
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
		} else {
			rf.convertToFollower(args.Term)
			rf.resetElectionTimeout(rf.me)
		}
		reply.Success = false
	} else {

	}
}

func (rf *Raft) sendRequestAppendEntry(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
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

func (rf *Raft) lastLogIndex() uint64 {
	return (uint64)(len(rf.log) - 1)
}

func (rf *Raft) resetLeaderHeastbeatsTimeout(server int) {
	rf.resetElectionTimeout(server)
}

func (rf *Raft) sendHeartbeats() {
	if rf.state != LEADER {
		return
	}

	args := &RequestAppendEntryArgs{rf.currentTerm, rf.me, rf.commitIndex, nil}
	for peer := 0; peer < len(rf.peers); peer++ {
		if rf.state != LEADER {
			return
		}
		if peer != rf.me {
			go func(server int, term int, args *RequestAppendEntryArgs) {
				var reply RequestAppendEntryReply
				Dbg(dLeader, "S%d send heartbeats to S%d T%d", rf.me, server, term)
				ok := rf.sendRequestAppendEntry(server, args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > term {
						rf.convertToFollower(reply.Term)
					}
				}
			}(peer, rf.currentTerm, args)
		} else {
			rf.resetLeaderHeastbeatsTimeout(peer)
		}
	}
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

	rf.convertToFollower(0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, LogEntry{-1, nil})
	rf.log = append(rf.log, LogEntry{rf.currentTerm, nil})
	rf.voteCh = make(chan bool)

	rand.New(rand.NewSource(time.Now().UnixNano()))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
