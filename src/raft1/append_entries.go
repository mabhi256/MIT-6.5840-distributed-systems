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

	// Step down if we're a candidate and see a leader
	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.becomeFollower(args.Term)
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Check log consistency at prevLogIndex
	prevEntry, exists := rf.logEntryAt(args.PrevLogIndex)

	// PrevLogIndex beyond our log
	if !exists && args.PrevLogIndex >= rf.logLength() {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.logLength()
		return
	}

	// PrevLogIndex moved to snapshot
	if !exists && args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}

	// PrevLogIndex at snapshot boundary
	if !exists && args.PrevLogIndex == rf.lastIncludedIndex {
		// But terms don't match
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.Success = false
			reply.XTerm = rf.lastIncludedTerm
			reply.XIndex = rf.lastIncludedIndex
			reply.XLen = rf.logLength()
			return
		}
		// Terms match, fall through to append
	}

	// Entry exists but term mismatch
	if exists && prevEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = prevEntry.Term
		reply.XIndex = rf.firstEntryIndexForTerm(reply.XTerm)
		reply.XLen = rf.logLength()

		// delete the existing entry and all that follow it
		rf.log = rf.logEntriesBefore(args.PrevLogIndex)
		rf.persist(nil)

		return
	}

	// truncate everything after matching PrevLogIndex and
	rf.log = rf.logEntriesBefore(args.PrevLogIndex + 1)
	// append new entries from the RPC args
	rf.log = append(rf.log, args.Entries...)
	rf.persist(nil)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logLastIndex())
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
