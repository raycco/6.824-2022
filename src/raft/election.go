package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const ElectionTimeout = 200 * time.Millisecond
const LeaderHeartbeatsTimeout = 80 * time.Millisecond
const TickInterval = 20 * time.Millisecond

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

func (args *RequestVoteArgs) str() string {
	return fmt.Sprintf("args:[T=%d LLI=%d LLT=%d]",
		args.Term, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (reply *RequestVoteReply) str() string {
	return fmt.Sprintf("reply:[T=%d GRANT=%t]", reply.Term, reply.VoteGranted)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nCurrentTerm := rf.currentTerm
	nLLIndex := rf.lastLogIndex()
	nLLTerm := rf.logEntryTerm(nLLIndex)

	LogPrint(INFO, dVote, "S%d [T=%d VF=%d LLI=%d LLT=%d ST=%d CI=%d] recv vote req from S%d %s",
		rf.me, rf.currentTerm, rf.votedFor, nLLIndex, nLLTerm,
		rf.state, rf.commitIndex, args.CandidateId, args.str())

	reply.Term = nCurrentTerm

	if args.Term > nCurrentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term <= nCurrentTerm || (rf.votedFor != -1 && args.CandidateId != rf.votedFor) {
		reply.VoteGranted = false
		return
	}

	if args.LastLogTerm < nLLTerm || (args.LastLogTerm == nLLTerm && args.LastLogIndex < nLLIndex) {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.resetElectionTimeout()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

func (rf *Raft) convertToLeader() {
	rf.state = LEADER
	rf.votedFor = -1

	// other algorithms must send redundant log entries to renumber them before they can be committed
	// rf.log = append(rf.log, LogEntry{rf.currentTerm, nil}) // no-op

	LogPrint(INFO, dLeader, "S%d victory at T%d, become leader", rf.me, rf.currentTerm)
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.lastLogIndex() + 1
		rf.matchIndex[peer] = rf.commitIndex
	}

	rf.persist()
}

func (rf *Raft) startElection() {

	rf.convertToCandidate()
	nVoteCount := 1

	rf.resetElectionTimeout() // two raft election timeout may same, then same always if not reset

	nLLIndex := rf.lastLogIndex()
	nLLTerm := rf.logEntryTerm(nLLIndex)

	args := &RequestVoteArgs{rf.currentTerm, rf.me, nLLIndex, nLLTerm}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			go func(server int, args *RequestVoteArgs) {
				var reply RequestVoteReply
				LogPrint(INFO, dVote, "S%d %s send vote req to S%d", rf.me, args.str(), server)
				ok := rf.sendRequestVote(server, args, &reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					LogPrint(INFO, dVote, "S%d [T=%d CNT=%d] args:[T=%d] recv vote res from S%d %s\n",
						rf.me, rf.currentTerm, nVoteCount, args.Term, server, reply.str())

					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					}

					if reply.VoteGranted {
						nVoteCount += 1
						if nVoteCount > len(rf.peers)/2 && args.Term == rf.currentTerm {
							nVoteCount = 0
							rf.convertToLeader()
							rf.sendHeartbeats()
						}
					}
				}
			}(peer, args)
		}
	}

	/*rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers)-1; i++ {

		select {
		case replyMsg := <-voteCh:
			Dbg(dVote, "S%d receive vote reply S%d RET %t", rf.me, replyMsg.server, replyMsg.ok)
			if replyMsg.ok {
				Dbg(dVote, "S%d receive vote reply S%d CNT %d, Grant %t\n", rf.me, replyMsg.server, rf.count, replyMsg.reply.VoteGranted)
				if replyMsg.reply.VoteGranted {
					rf.count += 1
					if rf.count > len(rf.peers)/2 {
						rf.electionEnd(false)
						return
					}
				} else {
					if rf.currentTerm < replyMsg.reply.Term {
						rf.currentTerm = replyMsg.reply.Term
					}
				}
			}
		case <-time.After(1000*time.Millisecond + time.Duration(rand.Intn(300))*time.Millisecond):
			Dbg(dTimer, "S%d election time out: CNT = %d, peers = %d\n", rf.me, rf.count, len(rf.peers))
			rf.electionEnd(true)
			return
		}
	}*/
}

func (rf *Raft) setElectionTimeout() {
	rf.electionTimeout = ElectionTimeout + time.Duration(rand.Intn(150))*time.Millisecond
	rf.electionTime = time.Now().Add(rf.electionTimeout)
	LogPrint(INFO, dTimer, "S%d reset election timeout=%v", rf.me, rf.electionTimeout)
}

func (rf *Raft) resetElectionTimeout() {
	rf.setElectionTimeout()
}
