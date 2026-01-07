package raft

import (
	"time"

	"6.5840/raftapi"
)

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Offset int // No offset for now
	Data   []byte
	Done   bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("[S%d][term-%d] Received InstallSnapshot RPC for index: %d\n", rf.me, rf.currentTerm, args.LastIncludedIndex)

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.becomeFollower(args.Term)
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reject stale snapshots
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// Keep entries after the snapshot
	newLog := make([]LogEntry, 0)
	for i := range rf.log {
		if rf.log[i].Index > args.LastIncludedIndex {
			newLog = append(newLog, rf.log[i])
		}
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// Update commitIndex if needed
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.persister.Save(rf.encodeRaftState(), args.Data)

	// Set pending snapshot - applyMsgSender will update lastApplied when it sends this
	rf.snapshotMsg = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Unlock()
	rf.applyCond.Signal()
}
