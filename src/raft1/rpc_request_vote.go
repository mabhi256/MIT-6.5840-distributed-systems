package raft

import (
	"math/rand/v2"
	"time"
)

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

func (rf *Raft) resetElectionTimeout() {
	timeout := ELECTION_TIMEOUT_MIN + rand.IntN(ELECTION_TIMEOUT_RANGE) // 300-500ms
	rf.electionTimeout = time.Duration(timeout) * time.Millisecond
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

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
	lastLogEntry := rf.lastLogEntry()
	if args.LastLogTerm != lastLogEntry.Term {
		isCandidateUpToDate = args.LastLogTerm > lastLogEntry.Term
	} else {
		isCandidateUpToDate = args.LastLogIndex >= lastLogEntry.Index
	}

	// Grant vote If votedFor is null (haven't voted for anybody) or votedFor is candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isCandidateUpToDate {
		rf.lastHeartbeat = time.Now()
		rf.resetElectionTimeout()

		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		return
	}
}
