package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) ResetElectionTime() {
	rf.electionTime = time.Now().Add(500*time.Millisecond + time.Duration(rand.Int()%200)*time.Millisecond)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DebugLog(dVote, "S%d T%d <- S%d T%d, get vote request", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastTerm := rf.currentTerm
	rf.UpdateTerm(args.Term)
	if args.Term < lastTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && LogUpToDate(rf.log.LastLogTerm(), rf.log.LastLogIndex(), args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	if reply.VoteGranted {
		rf.ResetElectionTime()
		DebugLog(dVote, "S%d -> S%d T%d, Granting Vot", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		DebugLog(dVote, "S%d Has voted for S%d at T%d", rf.me, rf.votedFor, rf.currentTerm)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartbeat + time.Duration((rand.Int()%50)*int(time.Millisecond)))

		rf.mu.Lock()
		if rf.state == Leader {
			rf.doLeader()
		}
		if rf.state == Follower {
			rf.doFollower()
		}
		if rf.state == Candidate {
			rf.doCandidate()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) doFollower() {
	if time.Now().After(rf.electionTime) {
		rf.state = Candidate
		DebugLog(dLeader, "S%d T%d Follower -> Candidate", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) doCandidate() {
	if time.Now().After(rf.electionTime) {
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.persist()
		rf.ResetElectionTime()
		votes := 1
		arg := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log.LastLogIndex(),
			LastLogTerm:  rf.log.LastLogTerm(),
		}
		DebugLog(dTimer, "S%d T%d Start vote", rf.me, rf.currentTerm)
		isleader := false
		for index := range rf.peers {
			if index != rf.me {
				go rf.CandidateRequestVotes(index, &votes, arg, &isleader)
			}
		}
	}
}

func (rf *Raft) doLeader() {
	rf.ResetElectionTime()
	rf.LeaderSendHeartbeats()
}

func (rf *Raft) CandidateRequestVotes(index int, votes *int, args *RequestVoteArgs, isleader *bool) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(index, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.UpdateTerm(reply.Term) {
		return
	}
	if ok && reply.VoteGranted && reply.Term == rf.currentTerm {
		*votes += 1
		DebugLog(dVote, "S%d T%d <- S%d T%d Get Vote", args.CandidateId, args.Term, index, reply.Term)
		if *votes > len(rf.peers)/2 && !(*isleader) && rf.state == Candidate {
			rf.state = Leader
			*isleader = true
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.nextIndex[i] = rf.log.LastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			DebugLog(dLeader, "S%d T%d Candidate -> Leader", rf.me, rf.currentTerm)
			rf.doLeader()
			DebugLog(dLeader, "S%d T%d Finish heartbeats after election", rf.me, rf.currentTerm)
		}
	}
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
