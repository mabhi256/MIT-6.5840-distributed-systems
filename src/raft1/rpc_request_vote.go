package raft

import "time"

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[S%d][term-%d][%s] received RequestVote from S%d for term %d\n", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm
	rf.lastHeartbeat = time.Now()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// If 2 logs entries have different terms, then the log with the later term is more up-to-date.
	// Else then longer log is more up-to-date.
	isCandidateUpToDate := false
	lastLogEntry := rf.log[len(rf.log)-1]
	if args.LastLogTerm != lastLogEntry.Term {
		isCandidateUpToDate = args.LastLogTerm > lastLogEntry.Term
	} else {
		isCandidateUpToDate = args.LastLogIndex >= lastLogEntry.Index
	}

	// Grant vote If votedFor is null (haven't voted for anybody) or votedFor is candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isCandidateUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimeout() // reset election timeout for each election
		return
	}
}
