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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[S%d][term-%d][%s] received AppendEntry from S%d for term %d\n", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.XTerm = rf.currentTerm
		return
	}

	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimeout()

	// If we're a candidate, and receive AppendEntries with same term
	// it means a leader has been elected - step down
	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.becomeFollower(args.Term)
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = rf.firstEntryIndexForTerm(reply.XTerm)
		reply.XLen = len(rf.log)

		return
	}

	for i, entry := range args.Entries {
		index := entry.Index - rf.firstLogEntry().Index
		if index >= len(rf.log) {
			// Entry doesn't exist, append it and all remaining
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		} else if rf.log[index].Term != entry.Term {
			// Conflict: delete this entry and all following, then append new entries
			rf.log = rf.log[:index]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		// else: entry matches, continue checking next entry
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
	rf.applyCond.Signal()
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

	return result
}

// Find the LAST index where log[i].Term == term
// Returns -1 if term not found
func (rf *Raft) lastEntryIndexForTerm(term int) int {
	left, right := 0, len(rf.log)-1
	result := -1

	for left <= right {
		mid := left + (right-left)/2

		if rf.log[mid].Term == term {
			result = mid // // Found it, but keep searching right for last occurence
			left = mid + 1
		} else if rf.log[mid].Term < term {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return result
}
