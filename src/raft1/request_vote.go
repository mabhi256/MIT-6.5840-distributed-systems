package raft

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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// If 2 logs entries have different terms, then the log with the later term is more up-to-date.
	// Else then longer log is more up-to-date.
	isCandidateUpToDate := false
	lastLogEntry := rf.logLastEntry()
	if args.LastLogTerm != lastLogEntry.Term {
		isCandidateUpToDate = args.LastLogTerm > lastLogEntry.Term
	} else {
		isCandidateUpToDate = args.LastLogIndex >= lastLogEntry.Index
	}

	if !isCandidateUpToDate {
		return
	}

	// Grant vote If votedFor is null (haven't voted for anybody) or votedFor is candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		rf.persist(nil)

		reply.VoteGranted = true
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
