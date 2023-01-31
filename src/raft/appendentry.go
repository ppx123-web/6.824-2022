package raft

import "time"

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

	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

// return true if term > rf.currentTerm
func (rf *Raft) UpdateTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) LeaderSendEntries(heartbeat bool) {
	for server := range rf.peers {
		if server == rf.me {
			rf.ResetElectionTime()
			continue
		}
		nextIndex := rf.nextIndex[server]
		if rf.log.LastLogIndex() >= nextIndex || heartbeat {
			if nextIndex <= rf.log.LastIncludedIndex {
				// go rf.SendInstallSnapshot(server, &InstallSnapshotArg{
				// 	Term:              rf.currentTerm,
				// 	LeaderId:          rf.me,
				// 	LastIncludedIndex: rf.log.LastIncludedIndex,
				// 	LastIncludedTerm:  rf.log.LastIncludedTerm,
				// 	Data:              rf.persister.ReadSnapshot(),
				// })
				// return
				nextIndex = rf.log.LastIncludedIndex + 1
			}
			prevLog := rf.log.get(nextIndex - 1)
			var args = AppendEntriesArg{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]LogEntry, rf.log.LastLogIndex()-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.LogToEnd(nextIndex))
			go rf.LeaderSendOneEntry(server, &args)
		}
	}

}

func (rf *Raft) LeaderSendOneEntry(server int, args *AppendEntriesArg) {
	var reply AppendEntriesReply
	rf.mu.Lock()
	DebugLog(dLog, "S%d T%d -> S%d Sending Entries PLI: %d PLT %d LC: %d, Entries Len %d", rf.me, rf.currentTerm, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.UpdateTerm(reply.Term)
			return
		}
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			DebugLog(dLog2, "S%d T%d Set S%d nextIndex to %d", rf.me, rf.currentTerm, server, rf.nextIndex[server])
		} else if reply.Conflict {
			//Conflict
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				var newNextIndex = 0
				for index := args.PrevLogIndex; index > rf.log.LastIncludedIndex; index-- {
					if rf.log.get(index).Term == reply.XTerm {
						newNextIndex = index + 1
						break
					}
				}
				if newNextIndex != 0 {
					rf.nextIndex[server] = newNextIndex
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}

			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			DebugLog(dDrop, "S%d T%d -> S%d Entry Conflict, current T%d", args.LeaderId, args.Term, server, rf.currentTerm)
			//retry
			nextIndex := rf.nextIndex[server]
			prevLog := rf.log.get(nextIndex - 1)
			var newargs = AppendEntriesArg{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]LogEntry, rf.log.LastLogIndex()-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(newargs.Entries, rf.log.LogToEnd(nextIndex))
			DebugLog(dDrop, "S%d T%d -> S%d Retry AppEnt PLI: %d PLT %d LC: %d ", rf.me, rf.currentTerm, server, newargs.PrevLogIndex, newargs.PrevLogTerm, newargs.LeaderCommit)
			go rf.LeaderSendOneEntry(server, &newargs)
		}
	}

}

func (rf *Raft) Committer() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			commit := rf.commitIndex
			for n := commit + 1; n <= rf.log.LastLogIndex(); n++ {
				sum := 1
				for id := range rf.peers {
					if id != rf.me && rf.matchIndex[id] >= n && rf.log.get(n).Term == rf.currentTerm {
						sum += 1
					}
				}
				if sum > len(rf.peers)/2 {
					DebugLog(dCommit, "S%d T%d commit to index %d", rf.me, rf.currentTerm, n)
					rf.commitIndex = n
				}
			}
		}
		rf.mu.Unlock()
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
	reply.Conflict = false
	if args.Term < lastTerm {
		//Reply false if term < currentTerm
		reply.Success = false
		DebugLog(dLog2, "S%d T%d -> S%d T%d reject AppEnt", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}
	if rf.state == Candidate {
		rf.state = Follower
	}
	rf.ResetElectionTime()
	//append entries
	//rule 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	prevLog := rf.log.get(args.PrevLogIndex)
	if prevLog == nil {
		reply.Success = false
		reply.Conflict = true
		reply.XLen = rf.log.LastLogIndex()
		reply.XTerm = -1
		DebugLog(dLog2, "S%d T%d -> S%d T%d ,AppEnt PLI %d PLT %d conflict, ConflictArg: Term:%d, Index:%d, Len:%d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Conflict = true
		reply.XTerm = rf.log.get(args.PrevLogIndex).Term
		reply.XIndex = 1
		for index := args.PrevLogIndex; index > rf.log.LastIncludedIndex; index-- {
			if rf.log.get(index).Term != reply.XTerm {
				reply.XIndex = index + 1
				break
			}
		}
		DebugLog(dLog2, "S%d T%d -> S%d T%d ,AppEnt PLI %d PLT %d conflict, ConflictArg: Term:%d, Index:%d, Len:%d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	//rule 3
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	nextIndex := len(args.Entries)
	for id, entry := range args.Entries {
		curId := args.PrevLogIndex + 1 + id
		if curId > rf.log.LastLogIndex() || entry.Index == rf.log.get(curId).Index && entry.Term != rf.log.get(curId).Term {
			rf.log.Log = rf.log.LogStartTo(curId)
			nextIndex = id
			break
		}
	}

	// rule 4: Append any new entries not already in the log
	for index := nextIndex; index < len(args.Entries); index++ {
		rf.log.Log = append(rf.log.Log, args.Entries[index])
	}
	rf.persist()
	// rule 5
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		DebugLog(dLog2, "S%d T%d Resetting commitIndex %d", rf.me, rf.currentTerm, rf.commitIndex)
	}
	if len(args.Entries) == 0 {
		DebugLog(dLog2, "S%d T%d Reply heartbeats True", rf.me, rf.currentTerm)
	} else {
		DebugLog(dLog2, "S%d T%d Reply AppEnt PLI %d PLT %d LL %d Success True", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	}
	reply.Success = true

}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		var tmpLog Logs
		tmpCommitIndex := rf.commitIndex
		tmpLog.LogCopy(&rf.log)
		// DebugLog(dTimer, "S%d T%d copy temp log, start %d, len %d", rf.me, rf.currentTerm, rf.log.LastIncludedIndex, rf.log.LogLength())
		for {
			if tmpCommitIndex > rf.lastApplied {
				rf.lastApplied += 1
				var msg = ApplyMsg{
					CommandValid: true,
					Command:      tmpLog.get(rf.lastApplied).Commands,
					CommandIndex: tmpLog.get(rf.lastApplied).Index,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.state == Leader {
					DebugLog(dTimer, "S%d T%d (Leader) apply, cmd %v, index %d", rf.me, rf.currentTerm, msg.Command, msg.CommandIndex)
				} else {
					DebugLog(dTimer, "S%d T%d (Follower) apply, cmd %v, index %d", rf.me, rf.currentTerm, msg.Command, msg.CommandIndex)
				}
			} else {
				break
			}
		}
		rf.mu.Unlock()
	}
}
