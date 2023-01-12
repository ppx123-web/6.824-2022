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

	"log"
	"math/rand"
	"os"
	"strconv"
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

// log[] in raft persistent
type LogEntry struct {
	Term     int
	Commands interface{}
	Index    int
}

func (rf *Raft) LastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) LastLog() LogEntry {
	return rf.log[rf.LastLogIndex()]
}

func LogUpToDate(lastLogTerm1, lastLogIndex1, lastLogTerm2, lastLogIndex2 int) bool {
	if lastLogTerm1 == lastLogTerm2 {
		return lastLogIndex1 <= lastLogIndex2
	} else {
		return lastLogTerm1 < lastLogTerm2
	}

}

type RaftState int

const (
	Follower RaftState = iota + 1
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state        RaftState
	leader       int
	heartbeat    time.Duration
	electionTime time.Time

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders (Reinitialized after election)

	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logger *log.Logger
}

func (rf *Raft) ResetElectionTime() {
	rf.electionTime = time.Now().Add(500*time.Millisecond + time.Duration(rand.Int()%200)*time.Millisecond)
}

// return true if term > rf.currentTerm
func (rf *Raft) UpdateTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = -1
		return true
	}
	return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	rf.logger.Println("Get RequestVote from", args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastTerm := rf.currentTerm
	rf.UpdateTerm(args.Term)
	if args.Term < lastTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && LogUpToDate(rf.LastLog().Term, rf.LastLogIndex(), args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	if reply.VoteGranted {
		rf.ResetElectionTime()
		rf.logger.Println("Vote for", args.CandidateId)
	} else {
		rf.logger.Println("Has voted for", rf.votedFor)
	}
}

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) LeaderSendHeartbeats(server int) {
	rf.mu.Lock()
	args := AppendEntriesArg{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	rf.sendAppendEntries(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader && rf.UpdateTerm(reply.Term) {
		rf.state = Follower
	}
}

func (rf *Raft) LeaderSendEntries() {
	for id := range rf.peers {
		if id == rf.me {
			rf.ResetElectionTime()
		}
		nextIndex := rf.nextIndex[id]
		if rf.LastLogIndex() >= nextIndex {
			prevLog := rf.log[nextIndex-1]
			var args = AppendEntriesArg{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]LogEntry, rf.LastLogIndex()-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log[nextIndex:])
			go rf.LeaderSendOneEntry(id, &args)
		}
	}

}

func (rf *Raft) LeaderSendOneEntry(server int, args *AppendEntriesArg) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Success {
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = len(rf.log) - 1
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			//decrement
			rf.nextIndex[server] -= 1
			//retry
			nextIndex := rf.nextIndex[server]
			prevLog := rf.log[nextIndex-1]
			args.PrevLogIndex = nextIndex - 1
			args.PrevLogTerm = prevLog.Term
			args.Entries = make([]LogEntry, rf.LastLogIndex()-nextIndex+1)
			copy(args.Entries, rf.log[nextIndex:])
			go rf.LeaderSendOneEntry(server, args)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastTerm := rf.currentTerm
	rf.UpdateTerm(args.Term)
	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 {
		//heartbeat
		if args.Term < lastTerm {
			reply.Success = false
			return
		} else {
			rf.ResetElectionTime()
		}
	} else {
		//append entries
		if rf.LastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
		for id, entry := range args.Entries {
			if entry.Term != rf.log[args.PrevLogIndex + id]
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
	// index = rf.log
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	index = rf.LastLogIndex() + 1
	var newLog = LogEntry{
		Term:     rf.currentTerm,
		Commands: command,
		Index:    index,
	}
	rf.log = append(rf.log, newLog)
	rf.LeaderSendEntries()
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

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader && rf.commitIndex > rf.lastApplied {
			commit := rf.commitIndex
			for n := commit + 1; n <= rf.LastLogIndex(); n++ {
				sum := 0
				for id := range rf.peers {
					if id != rf.me && rf.matchIndex[id] >= n && rf.log[n].Term == rf.currentTerm {
						sum += 1
					}
				}
				if n >= len(rf.peers)/2 {
					rf.lastApplied += 1
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[n].Commands,
						CommandIndex: rf.lastApplied,
					}
					rf.commitIndex = n
				}
			}
		}
		rf.mu.Unlock()
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
		rf.logger.Println("Follower -> Candidate")
	}
}

func (rf *Raft) doCandidate() {
	if time.Now().After(rf.electionTime) {
		// rf.logger.Println("")
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.ResetElectionTime()
		votes := 1
		arg := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.LastLogIndex(),
			LastLogTerm:  rf.LastLog().Term,
		}
		rf.logger.Println("Start vote, Term", rf.currentTerm)
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
	for index := range rf.peers {
		if index != rf.me {
			go rf.LeaderSendHeartbeats(index)
		}
	}
}

func (rf *Raft) CandidateRequestVotes(index int, votes *int, args *RequestVoteArgs, isleader *bool) {
	rf.logger.Println("Send RequestVote to", index)
	var reply RequestVoteReply
	ok := rf.sendRequestVote(index, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.VoteGranted {
		*votes += 1
		rf.logger.Println("Get Vote from", index)
		if *votes > len(rf.peers)/2 && !(*isleader) && rf.state == Candidate {
			rf.state = Leader
			*isleader = true
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.nextIndex[i] = rf.LastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			rf.logger.Println("Candidate -> Leader, Term", rf.currentTerm)
			rf.doLeader()
			rf.logger.Println("Leader finishes sending heartbeats")
		}
	} else {
		rf.UpdateTerm(reply.Term)
		return
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leader = -1
	rf.state = Follower
	rf.heartbeat = 100 * time.Millisecond //less than 10 heartbeats per second
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.ResetElectionTime()

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	f, _ := os.OpenFile("log.txt", os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)

	rf.logger = log.New(f, "", log.Lshortfile)
	rf.logger.SetPrefix("[" + strconv.Itoa(rf.me) + "] ")
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	rf.logger.Println("Init Finish")
	return rf
}
