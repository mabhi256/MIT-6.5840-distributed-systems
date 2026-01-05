package raft

import "time"

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[S%d][term-%d][%s] received AppendEntry from S%d for term %d\n", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term)

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
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
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for i, entry := range args.Entries {
		index := entry.Index - rf.log[0].Index
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

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Signal()
	}

	reply.Success = true
}
