package raft

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	// Max 10 hearbeat per sec
	HEARTBEAT_FREQ         = 100 * time.Millisecond
	ELECTION_TIMEOUT_MIN   = 300
	ELECTION_TIMEOUT_RANGE = 200
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	var str string
	switch s {
	case Follower:
		str = "Follower"
	case Candidate:
		str = "Candidate"
	case Leader:
		str = "Leader"
	}

	return str
}

// Index of first command is 1, index-0 is a sentinel heartbeat
type LogEntry struct {
	Command any
	Index   int
	Term    int // term when entry was received by leader
}

type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *tester.Persister   // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	applyCh       chan raftapi.ApplyMsg
	applyCond     *sync.Cond
	applySnapCond *sync.Cond

	state         State
	lastHeartbeat time.Time

	// Persistent state on ALL servers: (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int // peer id
	log         []LogEntry

	// Volatile state on ALL servers
	commitIndex int // Leader's commitIndex
	lastApplied int // Log ID applied to state machine

	// Volatile state on Leader: (Reinitialized after election)
	// peers[i] will receive leader's log[nextIndex[i]] next,
	// initialized to leader's last log index + 1,
	// then uses AppendEntries failures to decrement nextIndex[i]
	// and find where the follower's log actually diverges.
	nextIndex []int
	// highest log entry where peers[i]'s log[matchIndex[i]] = leader's log[matchIndex[i]]
	// And replicated i.e. AppendEntries RPC was successful.
	// Initialized to 0,
	matchIndex []int

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotMsg       *raftapi.ApplyMsg
}

func (rf *Raft) toPhysicalIndex(logicalIndex int) int {
	return logicalIndex - rf.lastIncludedIndex - 1
}

func (rf *Raft) getLastLogEntry() LogEntry {
	idx := len(rf.log) - 1
	if idx >= 0 {
		return rf.log[idx]
	}

	return LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}
}

func (rf *Raft) getLastLogIndex() int {
	lastLogEntry := rf.getLastLogEntry()
	return lastLogEntry.Index
}

func (rf *Raft) getLogLength() int {
	return rf.lastIncludedIndex + 1 + len(rf.log) // rf.getLastLogIndex() + 1
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(raftData []byte) {
	if len(raftData) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(raftData)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var entries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	err := d.Decode(&currentTerm)
	err = d.Decode(&votedFor)
	err = d.Decode(&entries)
	err = d.Decode(&lastIncludedIndex)
	err = d.Decode(&lastIncludedTerm)

	if err != nil {
		log.Fatalf("Err decoding: %s", err.Error())
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot up to and including index
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore stale snapshot requests
	if index <= rf.lastIncludedIndex {
		return
	}

	DPrintf("[S%d][term-%d][%s] Service called snapshot on index %d\n", rf.me, rf.currentTerm, rf.state, index)
	physicalIndex := rf.toPhysicalIndex(index)

	// Incorrect snapshot index
	if physicalIndex >= len(rf.log) {
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[physicalIndex].Term
	rf.log = rf.log[physicalIndex+1:]

	if rf.commitIndex < rf.lastIncludedIndex {
		fmt.Println("--------")
		rf.commitIndex = rf.lastIncludedIndex
	}

	if rf.state == Leader && rf.matchIndex[rf.me] < rf.lastIncludedIndex {
		fmt.Println("=======")
		rf.matchIndex[rf.me] = rf.lastIncludedIndex
	}

	rf.persist(snapshot)
}

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

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// If 2 logs entries have different terms, then the log with the later term is more up-to-date.
	// Else then longer log is more up-to-date.
	isCandidateUpToDate := false
	lastLogEntry := rf.getLastLogEntry()
	if args.LastLogTerm != lastLogEntry.Term {
		isCandidateUpToDate = args.LastLogTerm > lastLogEntry.Term
	} else {
		isCandidateUpToDate = args.LastLogIndex >= lastLogEntry.Index
	}

	// Grant vote If votedFor is null (haven't voted for anybody) or votedFor is candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isCandidateUpToDate {
		rf.lastHeartbeat = time.Now()
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
//
// look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
		rf.persist(nil)

		return
	}

	// truncate everything after matching PrevLogIndex and
	rf.log = rf.log[:physicalPrevLogIdx+1]
	// append new entries from the RPC args
	rf.log = append(rf.log, args.Entries...)
	rf.persist(nil)

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

	rf.persist(args.Data)

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader && rf.currentTerm == term {
		index = rf.getLastLogIndex() + 1 // logical index
		latestEntry := LogEntry{Command: command, Index: index, Term: term}
		rf.log = append(rf.log, latestEntry)
		rf.persist(nil)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		DPrintf("[S%d][term-%d][%s] command %v received for index: %d", rf.me, rf.currentTerm, rf.state, command, index)
		go rf.replicateToAll()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// Check if a leader election should be started.
	for rf.killed() == false {
		electionTimeout := ELECTION_TIMEOUT_MIN + rand.IntN(ELECTION_TIMEOUT_RANGE) // 300-500ms
		timeoutDuration := time.Duration(electionTimeout) * time.Millisecond

		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > timeoutDuration {
			go rf.startElection(timeoutDuration)
		}
		rf.mu.Unlock()

		time.Sleep(timeoutDuration)
	}
}

func (rf *Raft) startElection(timeoutDuration time.Duration) {
	ch := make(chan bool, len(rf.peers))

	rf.mu.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	rf.becomeCandidate()
	currentTerm := rf.currentTerm
	lastLogEntry := rf.getLastLogEntry()
	request := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func() {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, request, reply)
			if !ok {
				ch <- false
				return
			}

			rf.mu.Lock()
			if reply.VoteGranted {
				ch <- true
			} else {
				ch <- false
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
			}
			rf.mu.Unlock()
		}()
	}

	rf.tallyVotes(ctx, ch, currentTerm)
}

func (rf *Raft) tallyVotes(ctx context.Context, ch <-chan bool, currentTerm int) {
	majority := (len(rf.peers) + 1) / 2
	numVotes := 1

	for range len(rf.peers) - 1 {
		select {
		case vote := <-ch:
			if vote {
				numVotes++
				if numVotes >= majority {
					rf.mu.Lock()
					if rf.state == Candidate && currentTerm == rf.currentTerm {
						rf.becomeLeader()
					}
					rf.mu.Unlock()
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist(nil)
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(nil)
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	// Reinitialize nextIndex and matchIndex
	lastIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIndex

	go rf.heartbeatToAll()
}

func (rf *Raft) heartbeatToAll() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		// If we are no longer the leader, or the term changed,
		// then stop sending Heartbeat / Empty AppendEntries RPC
		if rf.state != Leader || currentTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go rf.heartbeatToPeer(i, currentTerm)
		}

		time.Sleep(HEARTBEAT_FREQ)
	}
}

// piggyback log replication on heartbeats to reduce RPC overhead
func (rf *Raft) heartbeatToPeer(server int, currentTerm int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != currentTerm {
		rf.mu.Unlock()
		return
	}

	prevLogIndex, prevLogTerm, _ := rf.prepareAppendEntries(rf.nextIndex[server])

	request := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{}, // Always empty for heartbeats
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, request, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if rf.state != Leader || rf.currentTerm != currentTerm {
		return
	}

	// If heartbeat reveals log mismatch, trigger replication
	if !reply.Success {
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			lastIndex := rf.lastEntryIndexForTerm(reply.XTerm)
			if lastIndex == -1 {
				rf.nextIndex[server] = reply.XIndex
			} else {
				rf.nextIndex[server] = lastIndex + 1
			}
		}
		// Trigger replication to fix the log
		go rf.replicateToPeer(server, currentTerm)
	}
}

func (rf *Raft) prepareAppendEntries(nextIndex int) (int, int, []LogEntry) {
	prevLogIndex := nextIndex - 1

	prevLogTerm := rf.lastIncludedTerm
	if prevLogIndex > rf.lastIncludedIndex {
		physicalPrevLogIdx := rf.toPhysicalIndex(prevLogIndex)
		prevLogTerm = rf.log[physicalPrevLogIdx].Term
	}

	entries := []LogEntry{}
	physicalNextIndex := rf.toPhysicalIndex(nextIndex)
	if physicalNextIndex >= 0 && physicalNextIndex < len(rf.log) {
		entries = slices.Clone(rf.log[physicalNextIndex:])
	}

	return prevLogIndex, prevLogTerm, entries
}

func (rf *Raft) replicateToAll() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.replicateToPeer(i, currentTerm)
	}
}

func (rf *Raft) replicateToPeer(server int, currentTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		// Follower lags too much
		snapshot := rf.persister.ReadSnapshot()
		if rf.nextIndex[server] <= rf.lastIncludedIndex && snapshot != nil {
			request := &InstallSnapshotArgs{
				Term:              currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Offset:            0,
				Data:              snapshot,
				Done:              true,
			}
			reply := &InstallSnapshotReply{}
			rf.mu.Unlock()

			ok := rf.sendInstallSnapshot(server, request, reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			rf.mu.Lock()
			if reply.Term > currentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			}

			if rf.state != Leader || rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}

			rf.matchIndex[server] = rf.lastIncludedIndex
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
		}

		prevLogIndex, prevLogTerm, entries := rf.prepareAppendEntries(rf.nextIndex[server])

		commitIndex := rf.commitIndex

		request := &AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()

		// Don't hold locks when sending RPC
		ok := rf.sendAppendEntries(server, request, reply)
		if !ok {
			// If sending/receving RPC failed, retry after sleep
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		if reply.Term > currentTerm {
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			break
		}

		if rf.state != Leader || rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		// If true update matchindex and next index for each peer
		// If majority true then append the latest command to log, increment last applied, and return to client
		if reply.Success {
			rf.nextIndex[server] = prevLogIndex + len(entries) + 1
			rf.matchIndex[server] = prevLogIndex + len(entries)

			// Try to advance commitIndex
			rf.updateCommitIndex()
			rf.mu.Unlock()
			break
		} else {
			// If false, decrement nextIndex and retry with more updated entries list
			if reply.XTerm == -1 {
				// Follower log is too short, set nextIndex = XLen (right after follower's log)
				rf.nextIndex[server] = reply.XLen
			} else {
				lastIndex := rf.lastEntryIndexForTerm(reply.XTerm)
				if lastIndex == -1 {
					// Leader doesn't have XTerm which starts at reply.XIndex,
					// set nextIndex = XIndex and skip the entries with XTerm in follower
					rf.nextIndex[server] = reply.XIndex
				} else {
					// Leader has XTerm but has conflicts with follower,
					// set nextIndex = leader's lastIndex for this term + 1,
					// so the follower gets all terms up to this
					rf.nextIndex[server] = lastIndex + 1
				}
			}

			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N s.t.
	// N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
		physicalIndex := rf.toPhysicalIndex(n)

		term := rf.lastIncludedTerm
		if physicalIndex >= 0 && physicalIndex < len(rf.log) {
			term = rf.log[physicalIndex].Term
		} else {
			continue
		}
		if term != rf.currentTerm {
			continue
		}

		count := 1 // count self
		for i := range rf.peers {
			if rf.me != i && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) applyMsgSender() {
	for !rf.killed() {
		rf.mu.Lock()

		// Wait until there's something to apply
		for rf.commitIndex <= rf.lastApplied && rf.snapshotMsg == nil {
			rf.applyCond.Wait()
		}

		var snapshotMsg *raftapi.ApplyMsg
		if rf.snapshotMsg != nil {
			// Only use the snapshot if it's newer than what we've applied
			if rf.snapshotMsg.SnapshotIndex > rf.lastApplied {
				snapshotMsg = rf.snapshotMsg
				// Update lastApplied to snapshot index
				rf.lastApplied = snapshotMsg.SnapshotIndex
			}
			// Clear the pending snapshot regardless
			rf.snapshotMsg = nil
		}

		var msgs []raftapi.ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
			}
			if rf.lastApplied >= rf.getLastLogIndex() {
				break
			}

			rf.lastApplied++
			physicalIndex := rf.toPhysicalIndex(rf.lastApplied)

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[physicalIndex].Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()

		if snapshotMsg != nil {
			// DPrintf("[S%d][term-%d][%s] applying snapshot at index: %d, term: %d",
			// 	rf.me, rf.currentTerm, rf.state, rf.snapshotMsg.SnapshotIndex, rf.snapshotMsg.SnapshotTerm)
			rf.applyCh <- *snapshotMsg
		}

		if len(msgs) > 0 {
			DPrintf("[S%d][term-%d][%s] applying commands: %v", rf.me, rf.currentTerm, rf.state, msgs)
		}
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		applyCh:   applyCh,
		me:        me,

		state:         Follower,
		lastHeartbeat: time.Now(),

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // start with a sentinel entry as raft uses 1-indexed log

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		lastIncludedIndex: -1,
		lastIncludedTerm:  0,
	}

	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start sender goroutine to wait for majority and apply log to state machine
	go rf.applyMsgSender()

	return rf
}
