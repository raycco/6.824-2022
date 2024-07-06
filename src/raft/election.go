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

const ElectionTimeout = 150 * time.Millisecond
const LeaderHeartbeatsTimeout = 100 * time.Millisecond

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

func (reqVoteArgs *RequestVoteArgs) str() string {
	return fmt.Sprintf("[T=%d LLI=%d LLT=%d]",
		reqVoteArgs.Term, reqVoteArgs.LastLogIndex, reqVoteArgs.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (reqVoteReply *RequestVoteReply) str() string {
	return fmt.Sprintf("[T=%d GRANT=%t]", reqVoteReply.Term, reqVoteReply.VoteGranted)
}

type VoteReplyMsg struct {
	ok     bool
	server int
	reply  RequestVoteReply
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//Dbg(dVote, "S%d RequestVote S%d T%d", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	myTerm := rf.currentTerm
	myLLIndex := rf.lastLogIndex()

	Dbg(dVote, "S%d [T=%d VF=%d LLI=%d LLT=%d ST=%d CI=%d] receive vote req from S%d %s",
		rf.me, rf.currentTerm, rf.votedFor, rf.lastLogIndex(), rf.log[myLLIndex].Term,
		rf.state, rf.commitIndex, args.CandidateId, args.str())

	reply.Term = myTerm

	if args.Term > myTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term <= myTerm || (rf.votedFor != -1 && args.CandidateId != rf.votedFor) {
		reply.VoteGranted = false
		return
	}

	if args.LastLogTerm < rf.log[myLLIndex].Term ||
		(args.LastLogTerm == rf.log[myLLIndex].Term && args.LastLogIndex < myLLIndex) {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.resetElectionTimeout(rf.me)
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

	Dbg(dLeader, "S%d victory T%d", rf.me, rf.currentTerm)
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.lastLogIndex() + 1
		rf.matchIndex[peer] = rf.commitIndex
	}
}

func (rf *Raft) startElection() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.convertToCandidate()
	voteCount := 1

	myLLIndex := rf.lastLogIndex()
	args := &RequestVoteArgs{rf.currentTerm, rf.me, myLLIndex, rf.log[myLLIndex].Term}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			go func(server int, args *RequestVoteArgs) {
				var reply RequestVoteReply
				Dbg(dVote, "S%d %s send vote request to S%d", rf.me, args.str(), server)
				ok := rf.sendRequestVote(server, args, &reply)

				//rf.voteCh <- VoteReplyMsg{ok, server, reply}
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					Dbg(dVote, "S%d [T=%d CNT=%d ArgsT=%d] receive vote reply from S%d %s\n",
						rf.me, rf.currentTerm, voteCount, args.Term, server, reply.str())

					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					}

					if reply.VoteGranted {
						voteCount += 1
						if voteCount > len(rf.peers)/2 && args.Term == rf.currentTerm {
							voteCount = 0
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
}

func (rf *Raft) resetElectionTimeout(server int) {

	go func(server int) {
		Dbg(dTimer, "S%d reset election timeout", server)
		rf.voteCh <- true
	}(server)
}

func (rf *Raft) doElection() {

	select {
	case <-rf.voteCh:
		rf.mu.Lock()
		rf.lastElectionTimeout = rf.electionTimeout
		rf.setElectionTimeout()
		Dbg(dTimer, "S%d reset election timeout=%d", rf.me, rf.electionTimeout/time.Millisecond)
		rf.mu.Unlock()

	case <-time.After(rf.electionTimeout):
		rf.mu.Lock()
		Dbg(dTimer, "S%d timeout, next election", rf.me)
		if rf.lastElectionTimeout == rf.electionTimeout {
			rf.startElection()
		} else {
			rf.lastElectionTimeout = rf.electionTimeout
		}
		rf.mu.Unlock()
	}
}
