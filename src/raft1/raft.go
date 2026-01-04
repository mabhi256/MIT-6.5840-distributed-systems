package raft

import (
	"bytes"
	"context"
	"log"
	"math/rand/v2"
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

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist(nil)
	rf.lastHeartbeat = time.Now()
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
	lastIndex := rf.logLastIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIndex

	go rf.heartbeatToAll()
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

	DPrintf("[S%d][term-%d][%s] Service called snapshot on index %d\n", rf.me, rf.currentTerm, rf.state, index)
	entry, exists := rf.logEntryAt(index)
	if !exists {
		return
	}

	rf.log = rf.logEntriesAfter(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = entry.Term

	rf.persist(snapshot)
}

func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader && rf.currentTerm == term {
		index = rf.logLastIndex() + 1 // logical index
		latestEntry := LogEntry{Command: command, Index: index, Term: term}
		rf.log = append(rf.log, latestEntry)
		rf.persist(nil)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		DPrintf("[S%d][term-%d][%s] START: Added cmd=%v at index=%d",
			rf.me, rf.currentTerm, rf.state, command, index)
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
	// Wake up any goroutines waiting on condition variables
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// Check if a leader election should be started.
	for !rf.killed() {
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

// Election orchestration - called by ticker
func (rf *Raft) startElection(timeoutDuration time.Duration) {
	ch := make(chan bool, len(rf.peers))

	rf.mu.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	rf.becomeCandidate()
	currentTerm := rf.currentTerm
	lastLogEntry := rf.logLastEntry()
	rf.mu.Unlock()
	request := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}

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
			if reply.Term > currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()

			ch <- reply.VoteGranted
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

func (rf *Raft) prepareAppendEntries(nextIndex int) (int, int, []LogEntry) {
	prevLogIndex := nextIndex - 1
	entry, ok := rf.logEntryAt(prevLogIndex)

	prevLogTerm := rf.lastIncludedTerm
	if ok {
		prevLogTerm = entry.Term
	}

	entries := rf.logEntriesAfter(prevLogIndex)

	return prevLogIndex, prevLogTerm, entries
}

func (rf *Raft) backupNextIndex(server int, reply *AppendEntriesReply) {
	if reply.Success {
		return
	}

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
			// Leader has XTerm but follower has more entries for this term than the leader,
			// set nextIndex = leader's lastIndex for this term + 1,
			rf.nextIndex[server] = lastIndex + 1
		}
	}
}

// piggyback log replication on heartbeats to reduce RPC overhead
func (rf *Raft) heartbeatToPeer(server int, currentTerm int) {
	rf.mu.Lock()
	if rf.currentTerm != currentTerm {
		rf.mu.Unlock()
		return
	}

	prevLogIndex, prevLogTerm, _ := rf.prepareAppendEntries(rf.nextIndex[server])
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	request := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{}, // Always empty for heartbeats
		LeaderCommit: commitIndex,
	}
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, request, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != currentTerm {
		return
	}

	if reply.Term > currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// If heartbeat reveals log mismatch, trigger replication
	if !reply.Success {
		rf.backupNextIndex(server, reply)

		go rf.replicateToPeer(server, currentTerm)
	}
}

func (rf *Raft) replicateToPeer(server int, currentTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		// Follower lags too much
		snapshot := rf.persister.ReadSnapshot()
		lastIncludedIndex := rf.lastIncludedIndex
		lastIncludedTerm := rf.lastIncludedTerm

		if rf.nextIndex[server] <= lastIncludedIndex && snapshot != nil {
			rf.mu.Unlock()
			request := &InstallSnapshotArgs{
				Term:              currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Offset:            0,
				Data:              snapshot,
				Done:              true,
			}
			reply := &InstallSnapshotReply{}

			ok := rf.sendInstallSnapshot(server, request, reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			rf.mu.Lock()

			if rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}
			if reply.Term > currentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			}

			rf.matchIndex[server] = lastIncludedIndex
			rf.nextIndex[server] = lastIncludedIndex + 1
			rf.mu.Unlock()
			continue
		}

		prevLogIndex, prevLogTerm, entries := rf.prepareAppendEntries(rf.nextIndex[server])
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		request := &AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}
		reply := &AppendEntriesReply{}

		// Don't hold locks when sending RPC
		ok := rf.sendAppendEntries(server, request, reply)
		if !ok {
			// If sending/receving RPC failed, retry after sleep
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.mu.Lock()

		if rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}
		if reply.Term > currentTerm {
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			break
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
			rf.backupNextIndex(server, reply)

			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N s.t.
	// N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for n := rf.logLastIndex(); n > rf.commitIndex; n-- {
		entry, exists := rf.logEntryAt(n)
		if !exists || entry.Term != rf.currentTerm {
			continue
		}

		count := 1 // count self
		for i := range rf.peers {
			if rf.me != i && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			DPrintf("[S%d][term-%d][%s] COMMIT_ADVANCE: %d -> %d (term=%d, cmd=%v, matchIndex=%v)",
				rf.me, rf.currentTerm, rf.state, rf.commitIndex, n, entry.Term, entry.Command, rf.matchIndex)
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

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		var snapshotMsg *raftapi.ApplyMsg
		if rf.snapshotMsg != nil {
			// Only use the snapshot if it's newer than what we've applied
			if rf.snapshotMsg.SnapshotIndex > rf.lastApplied {
				snapshotMsg = rf.snapshotMsg
				// Update lastApplied to snapshot index
				rf.lastApplied = snapshotMsg.SnapshotIndex
				DPrintf("[S%d][term-%d][%s] applying snapshot at index: %d, term: %d",
					rf.me, rf.currentTerm, rf.state, rf.snapshotMsg.SnapshotIndex, rf.snapshotMsg.SnapshotTerm)
			}
			// Clear the pending snapshot regardless
			rf.snapshotMsg = nil
		}

		var msgs []raftapi.ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
			}

			rf.lastApplied++
			entry, exists := rf.logEntryAt(rf.lastApplied)
			if !exists {
				rf.lastApplied--
				break
			}

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()

		if snapshotMsg != nil {
			rf.applyCh <- *snapshotMsg
		}

		if len(msgs) > 0 {
			DPrintf("[S%d][term-%d][%s] applying %d commands to state machine",
				rf.me, rf.currentTerm, rf.state, len(msgs))
		}
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

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

	// start sender goroutine to wait for majority and apply logs/snapshot to state machine
	go rf.applyMsgSender()

	return rf
}
