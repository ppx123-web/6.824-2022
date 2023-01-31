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

	//	"6.824/labgob"

	"6.824/labgob"
)

type InstallSnapshotArg struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Offset            int
	// Done              bool
	// unused
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArg) (bool, *InstallSnapshotReply) {
	rf.mu.Lock()
	DebugLog(dSnap, "S%d T%d Send snapshot to S%d, LII %d, LIT %d", rf.me, rf.currentTerm, server, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	ok := rf.SendInstallSnapshotRPC(server, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.UpdateTerm(reply.Term) {
		return ok, &reply
	}
	if rf.currentTerm != args.Term || rf.state != Leader {
		return ok, &reply
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok, &reply
}

func (rf *Raft) SendInstallSnapshotRPC(server int, args *InstallSnapshotArg, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.UpdateTerm(args.Term) {
		return
	}
	if args.Term != rf.currentTerm {
		return
	}
	rf.ResetElectionTime()

	prevLastIncludeIndex := rf.log.LastIncludedIndex
	DebugLog(dSnap, "S%d T%d Get snapshot from S%d T%d, LII %d, LIT %d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.LastIncludedIndex > rf.commitIndex && args.LastIncludedIndex >= prevLastIncludeIndex {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(args.Data)
		rf.persister.SaveStateAndSnapshot(rf.getPersistData(), w.Bytes())
		DebugLog(dSnap, "S%d T%d update snapshot file, LII %d, LIT %d", rf.me, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
		if args.LastIncludedIndex <= rf.log.LastIncludedIndex && args.LastIncludedTerm == rf.log.get(args.LastIncludedIndex).Term {
			// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
			DebugLog(dSnap, "S%d T%d snapshot retain log", rf.me, rf.currentTerm)
			return
		}

		rf.log.Log = make([]LogEntry, 0)
		rf.commitIndex = args.LastIncludedIndex

		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}

		go func() {
			rf.applyCh <- msg
		}()
	}

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	// Previously, this lab recommended that you implement a function called CondInstallSnapshot to avoid the requirement that snapshots and log entries sent on applyCh are coordinated. This vestigal API interface remains, but you are discouraged from implementing it: instead, we suggest that you simply have it return true.
	DebugLog(dSnap, "S%d T%d apply snapshot", rf.me, rf.currentTerm)
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
	if rf.log.LastIncludedIndex >= index || index > rf.lastApplied || index > rf.log.LastLogIndex() {
		DebugLog(dSnap, "S%d T%d Snapshot abort, index %d, applied %d", rf.me, rf.currentTerm, index, rf.lastApplied)
		return
	}

	newStart := index
	tmpLogs := Logs{
		Log:               rf.log.LogToEnd(newStart + 1),
		LastIncludedIndex: newStart,
		LastIncludedTerm:  rf.log.get(newStart).Term,
	}
	rf.log.LogCopy(&tmpLogs)
	DebugLog(dSnap, "S%d T%d Snapshot index %d, log start at %d, log length %d", rf.me, rf.currentTerm, index, rf.log.LastIncludedIndex, rf.log.LogLength())

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)

}
