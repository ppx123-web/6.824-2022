package raft

import (
	"bytes"

	"6.824/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
	DebugLog(dPersist, "S%d T%d persist success, Log Length %d, Start %d, End %d", rf.me, rf.currentTerm, rf.log.LogLength(), rf.log.LastIncludedIndex, rf.log.LastLogIndex())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Logs
	e1 := d.Decode(&currentTerm)
	e2 := d.Decode(&votedFor)
	e3 := d.Decode(&log)
	if e1 != nil || e2 != nil || e3 != nil {
		DebugLog(dPersist, "S%d T%d Read persistant fails, T:%v V:%v L:%v", rf.me, rf.currentTerm, e1, e2, e3)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log.LogCopy(&log)
		rf.commitIndex = rf.log.LastIncludedIndex
		rf.lastApplied = rf.log.LastIncludedIndex
		DebugLog(dPersist, "S%d T%d Read persistant Success, log length %d", rf.me, rf.currentTerm, rf.log.LogLength())
	}
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e1 := e.Encode(rf.currentTerm)
	e2 := e.Encode(rf.votedFor)
	e3 := e.Encode(rf.log)
	if e1 != nil || e2 != nil || e3 != nil {
		DebugLog(dPersist, "S%d T%d get persist data fail, %v %v %v", rf.me, rf.currentTerm, e1, e2, e3)
	}
	return w.Bytes()
}
