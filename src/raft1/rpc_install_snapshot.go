package raft

import (
	"time"
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
	defer rf.mu.Unlock()

	DPrintf("[S%d][term-%d] Received InstallSnapshot RPC for index: %d\n", rf.me, rf.currentTerm, args.LastIncludedIndex)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.lastHeartbeat = time.Now()
	// rf.resetElectionTimeout()

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.becomeFollower(args.Term)
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// 6. If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	newLog := []LogEntry{{
		Command: nil,
		Index:   args.LastIncludedIndex,
		Term:    args.LastIncludedTerm,
	}}

	// idx := rf.findIdxForEntry(newLog[0])
	// if idx != -1 {
	// 	newLog = append(newLog, rf.log[idx+1:]...)
	// }
	for i, entry := range rf.log {
		if entry.Index == args.LastIncludedIndex && entry.Term == args.LastIncludedTerm {
			// Found matching entry - keep everything after it
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}

	// 7. Discard the entire log
	rf.log = newLog

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persister.Save(rf.encodeRaftState(), args.Data)

	rf.isSnapshotAvailable = true

	// Signal the applier to process the snapshot
	rf.applyCond.Signal()
}
