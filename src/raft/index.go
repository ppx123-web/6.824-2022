package raft

// log[] in raft persistent
type LogEntry struct {
	Term     int
	Commands interface{}
	Index    int // log starting at some index
}

type Logs struct {
	Log               []LogEntry
	LastIncludedIndex int // the first log's index
	LastIncludedTerm  int
}

func (l *Logs) LastLogIndex() int {
	if l.LogLength() == 0 {
		return l.LastIncludedIndex
	}
	return l.LastLog().Index
}

func (l *Logs) LastLogTerm() int {
	if l.LogLength() == 0 {
		return l.LastIncludedTerm
	}
	return l.LastLog().Term
}

func (l *Logs) LastLog() LogEntry {
	if l.LogLength() == 0 {
		return LogEntry{Term: l.LastIncludedTerm, Index: l.LastIncludedIndex}
	}
	return l.Log[len(l.Log)-1]
}

func (l *Logs) LogLength() int {
	return len(l.Log)
}

func (l *Logs) get(index int) *LogEntry {
	id := index - l.LastIncludedIndex - 1
	if index == l.LastIncludedIndex {
		return &LogEntry{Term: l.LastIncludedTerm, Index: l.LastIncludedIndex}
	}
	if id < 0 || index > l.LogLength()+l.LastIncludedIndex {
		return nil
	}
	return &l.Log[id]
}

func (l *Logs) LogAllocate(term int, cmd interface{}) {
	var index int
	if l.LogLength() == 0 {
		index = l.LastIncludedIndex + 1
	} else {
		index = l.LastLogIndex() + 1
	}
	l.Log = append(l.Log, LogEntry{
		Term:     term,
		Commands: cmd,
		Index:    index,
	})
}

func (l *Logs) LogCopy(src *Logs) {
	l.LastIncludedIndex = src.LastIncludedIndex
	l.LastIncludedTerm = src.LastIncludedTerm
	l.Log = make([]LogEntry, src.LogLength())
	copy(l.Log, src.Log)
}

func (l *Logs) LogToEnd(index int) []LogEntry {
	return l.Log[index-l.LastIncludedIndex-1:]
}

func (l *Logs) LogStartTo(index int) []LogEntry {
	return l.Log[:index-l.LastIncludedIndex-1]
}

func LogUpToDate(lastLogTerm1, lastLogIndex1, lastLogTerm2, lastLogIndex2 int) bool {
	if lastLogTerm1 == lastLogTerm2 {
		return lastLogIndex1 <= lastLogIndex2
	} else {
		return lastLogTerm1 < lastLogTerm2
	}

}
