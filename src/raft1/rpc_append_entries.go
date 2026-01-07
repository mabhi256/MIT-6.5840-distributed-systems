package raft

import "time"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// Log entry immediately preceding the LogEntry in Entries[0],
	// if no value in entries then send the last index/term
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry // empty for heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// AppendEntries RPC handler.Received by follower, to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[S%d][term-%d][%s] received AppendEntry from S%d for term %d\n", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term)

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.XTerm = rf.currentTerm
		return
	}

	// If we're a candidate, and receive AppendEntries with same term
	// it means a leader has been elected - step down
	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.becomeFollower(args.Term)
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	physicalPrevLogIdx := rf.toPhysicalIndex(args.PrevLogIndex)
	if physicalPrevLogIdx >= len(rf.log) {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.getLogLength()
		return
	}

	// PrevLogIndex is in the discarded snapshot
	if physicalPrevLogIdx < -1 { // args.PrevLogIndex < rf.lastIncludedIndex
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}

	if physicalPrevLogIdx == -1 { // args.PrevLogIndex == rf.lastIncludedIndex
		// Reject if terms don't match
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.Success = false
			reply.XTerm = rf.lastIncludedTerm
			reply.XIndex = rf.lastIncludedIndex
			reply.XLen = rf.getLogLength()
			return
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if physicalPrevLogIdx >= 0 && rf.log[physicalPrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[physicalPrevLogIdx].Term
		reply.XIndex = rf.firstEntryIndexForTerm(reply.XTerm)
		reply.XLen = rf.getLogLength()

		rf.log = rf.log[:physicalPrevLogIdx]
		// Adjust commitIndex if needed, since we truncated rf.log
		// The old leader sent AppendEntries [3,4,5] with LeaderCommit 5
		// Follower sets: commitIndex = 5, log = [0,1,2,3,4,5] but the lastApplied is still 2
		// Leader was network partitioned before it could gain majority on the new log and
		// apply the new changes to state machine
		// New leader elected, sends AppendEntries with conflict at index 3
		// Follower must truncate log to [0,1,2], but commitIndex is still 5
		lastLogIndex := rf.getLastLogIndex()
		if rf.commitIndex > lastLogIndex {
			rf.commitIndex = lastLogIndex
		}
		rf.persist()

		return
	}

	// truncate everything after matching PrevLogIndex and
	rf.log = rf.log[:physicalPrevLogIdx+1]
	// append new entries from the RPC args
	rf.log = append(rf.log, args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}

	reply.Success = true
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
		return -1
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
		return -1
	}
	return rf.log[result].Index
}
