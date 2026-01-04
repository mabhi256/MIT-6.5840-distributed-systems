package raft

func (rf *Raft) toPhysicalIndex(logicalIndex int) int {
	return logicalIndex - rf.lastIncludedIndex - 1
}

func (rf *Raft) logLastEntry() LogEntry {
	idx := len(rf.log) - 1
	if idx >= 0 {
		return rf.log[idx]
	}

	return LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
}

func (rf *Raft) logLastIndex() int {
	lastLogEntry := rf.logLastEntry()
	return lastLogEntry.Index
}

func (rf *Raft) logLength() int {
	return rf.lastIncludedIndex + 1 + len(rf.log) // rf.getLastLogIndex() + 1
}

func (rf *Raft) logEntryAt(index int) (*LogEntry, bool) {
	physicalIndex := rf.toPhysicalIndex(index)

	if physicalIndex < 0 || physicalIndex >= len(rf.log) {
		return nil, false
	}

	return &rf.log[physicalIndex], true
}

// Log entries (not including) after this index
func (rf *Raft) logEntriesAfter(logicalIdx int) []LogEntry {
	// Before snapshot: return entire log
	physicalIdx := rf.toPhysicalIndex(logicalIdx)
	if physicalIdx < 0 {
		return rf.log
	}

	if physicalIdx >= len(rf.log) {
		return []LogEntry{}
	}

	return rf.log[physicalIdx+1:]
}

// Log entries before (not including) this index
func (rf *Raft) logEntriesBefore(logicalIdx int) []LogEntry {
	physicalIdx := rf.toPhysicalIndex(logicalIdx)
	if physicalIdx < 0 {
		return []LogEntry{}
	}

	if physicalIdx >= len(rf.log) {
		return rf.log
	}

	return rf.log[:physicalIdx]
}

// Find the FIRST index where log[i].Term == term
// Returns -1 if term not found
func (rf *Raft) firstEntryIndexForTerm(term int) int {
	left, right := 0, len(rf.log)-1
	result := -1

	for left <= right {
		mid := left + (right-left)/2

		if rf.log[mid].Term == term {
			result = mid // Found it, but keep searching left for 1st occurence
			right = mid - 1
		} else if rf.log[mid].Term < term {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if result < 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[result].Index
}

// Find the LAST index where log[i].Term == term
// Returns -1 if term not found
func (rf *Raft) lastEntryIndexForTerm(term int) int {
	left, right := 0, len(rf.log)-1
	result := -1

	for left <= right {
		mid := left + (right-left)/2

		if rf.log[mid].Term == term {
			result = mid // Found it, but keep searching right for last occurence
			left = mid + 1
		} else if rf.log[mid].Term < term {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if result < 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[result].Index
}
