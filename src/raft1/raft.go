package raft

import (
	"bytes"
	"context"
	"log"
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
	RETRY_FREQ             = 10 * time.Millisecond
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

	state           State
	lastHeartbeat   time.Time
	electionTimeout time.Duration

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

	snapshotMsg *raftapi.ApplyMsg
}

func (rf *Raft) firstLogEntry() LogEntry {
	if len(rf.log) == 0 {
		panic("log should atleast have a sentinel. This should NEVER happen.")
	}

	return rf.log[0]
}

func (rf *Raft) lastIncludedIndex() int {
	return rf.firstLogEntry().Index
}

func (rf *Raft) lastIncludedTerm() int {
	return rf.firstLogEntry().Term
}

func (rf *Raft) lastLogEntry() LogEntry {
	if len(rf.log) == 0 {
		panic("log should atleast have a sentinel. This should NEVER happen.")
	}

	return rf.log[len(rf.log)-1]
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	// Reinitialize nextIndex and matchIndex
	lastIndex := rf.lastLogEntry().Index
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIndex

	go rf.replicateToAll(true)
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeRaftState(), rf.persister.ReadSnapshot())
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

	err := d.Decode(&currentTerm)
	err = d.Decode(&votedFor)
	err = d.Decode(&entries)

	if err != nil {
		log.Fatalf("Err decoding: %s", err.Error())
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries
		rf.lastApplied = rf.lastIncludedIndex()
		rf.commitIndex = rf.lastIncludedIndex()
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
	if index <= rf.lastIncludedIndex() {
		return
	}

	DPrintf("[S%d][term-%d][%s] Service called snapshot on index %d\n", rf.me, rf.currentTerm, rf.state, index)
	snapshotIdx := index - rf.lastIncludedIndex()
	rf.log = slices.Clone(rf.log[snapshotIdx:]) // keep snapshotIdx as sentinel, remove previous entries
	rf.log[0].Command = nil

	rf.persister.Save(rf.encodeRaftState(), snapshot)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan bool) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		ch <- false
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}
	rf.mu.Unlock()

	ch <- reply.VoteGranted
}

func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()

	index := -1
	term, isLeader := rf.GetState()

	if rf.state == Leader && rf.currentTerm == term {
		index = rf.lastLogEntry().Index + 1
		latestEntry := LogEntry{Command: command, Index: index, Term: term}
		rf.matchIndex[rf.me] = index
		rf.log = append(rf.log, latestEntry)
		rf.persist()
		rf.mu.Unlock()
		DPrintf("[S%d][term-%d][%s] command %v received for index: %d", rf.me, rf.currentTerm, rf.state, command, index)

		rf.replicateToAll(false)
	} else {
		DPrintf("[S%d][term-%d][%s] Received command: %v", rf.me, rf.currentTerm, rf.state, command)
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

func (rf *Raft) replicateToAll(isHeartbeat bool) {
	// lock once for all peers
	rf.mu.Lock()

	requests := make([]any, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		requests[i] = rf.prepareReplicationArgs(i)
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// piggyback log replication on heartbeats to reduce RPC overhead
		go rf.sendReplicationRPC(i, requests[i], isHeartbeat)
	}
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
		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()

		if state == Leader {
			rf.replicateToAll(true)
		} else if time.Since(lastHeartbeat) > electionTimeout {
			rf.startElection()
		}

		time.Sleep(HEARTBEAT_FREQ)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), rf.electionTimeout)
	defer cancel()
	ch := make(chan bool, len(rf.peers))

	rf.becomeCandidate()
	currentTerm := rf.currentTerm
	lastLogEntry := rf.lastLogEntry()
	request := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	rf.mu.Unlock()

	DPrintf("[S%d][term-%d][%s] sending RequestVote RPCs", rf.me, currentTerm, rf.state)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendRequestVote(i, request, ch)
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
					DPrintf("[S%d][term-%d][%s] got majority", rf.me, rf.currentTerm, rf.state)
					rf.mu.Lock()
					if rf.state == Candidate && currentTerm == rf.currentTerm {
						rf.becomeLeader()
						DPrintf("[S%d][term-%d][%s] got majority", rf.me, rf.currentTerm, rf.state)
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

func (rf *Raft) prepareReplicationArgs(server int) any {
	if rf.nextIndex[server] <= rf.lastIncludedIndex() {
		return rf.prepareInstallSnapshot()
	} else {
		return rf.prepareAppendEntries(server)
	}
}

func (rf *Raft) prepareInstallSnapshot() *InstallSnapshotArgs {
	request := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex(),
		LastIncludedTerm:  rf.lastIncludedTerm(),
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}

	return request
}

func (rf *Raft) prepareAppendEntries(server int) *AppendEntriesArgs {
	nextIdx := rf.nextIndex[server] - rf.lastIncludedIndex()
	prevLogEntry := rf.log[nextIdx-1]
	entries := slices.Clone(rf.log[nextIdx:])

	request := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogEntry.Index,
		PrevLogTerm:  prevLogEntry.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	return request
}

func (rf *Raft) sendReplicationRPC(server int, request any, isHeartbeat bool) {
	switch request := request.(type) {
	case *InstallSnapshotArgs:
		rf.sendInstallSnapshot(server, request, isHeartbeat)
	case *AppendEntriesArgs:
		rf.sendAppendEntries(server, request, isHeartbeat)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, isHeartbeat bool) {
	reply := &InstallSnapshotReply{}

	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if !ok {
			if isHeartbeat {
				return
			}
			time.Sleep(RETRY_FREQ)
			continue
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > args.Term {
			rf.becomeFollower(reply.Term)
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1

		request := rf.prepareReplicationArgs(server)
		go rf.sendReplicationRPC(server, request, false)
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, isHeartbeat bool) {
	reply := &AppendEntriesReply{}

	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			if isHeartbeat {
				return
			}
			// If sending/receving RPC failed, retry after sleep
			time.Sleep(RETRY_FREQ)
			continue
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != args.Term || rf.state != Leader {
			return
		}

		if reply.Term > args.Term {
			rf.becomeFollower(reply.Term)
			return
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			// DPrintf("[S%d][term-%d][%s] replicated by S%d up to %v", rf.me, rf.currentTerm, rf.state, server, rf.log[rf.matchIndex[server]])

			rf.updateCommitIndex()
			return
		} else {
			rf.backupNextIndex(server, reply)

			if isHeartbeat {
				request := rf.prepareReplicationArgs(server)
				go rf.sendReplicationRPC(server, request, false)
			}
			return
		}
	}
}

func (rf *Raft) backupNextIndex(server int, reply *AppendEntriesReply) {
	if reply.XTerm == -1 {
		// Follower log is too short, set nextIndex = XLen (right after follower's log)
		rf.nextIndex[server] = reply.XLen
	} else {
		lastIndex := rf.lastIdxForTerm(reply.XTerm)
		if lastIndex == -1 {
			// Leader doesn't have XTerm which starts at reply.XIndex,
			// set nextIndex = XIndex, the term's 1st index and skip all the entries with XTerm in follower
			rf.nextIndex[server] = reply.XIndex
		} else {
			// Leader has XTerm but has less entries for the term than the follower
			// set nextIndex = leader's lastIndex for this term + 1,
			// Ex. Follower has [3,4,4,5,5,5] but Leader has [3,4,4,5]
			rf.nextIndex[server] = rf.log[lastIndex].Index + 1
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N s.t.
	// N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for n := rf.lastLogEntry().Index; n > rf.commitIndex; n-- {
		idx := n - rf.lastIncludedIndex()
		if rf.log[idx].Term != rf.currentTerm {
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
			snapshotMsg = rf.snapshotMsg
			// Update lastApplied to snapshot index
			// rf.lastApplied = snapshotMsg.SnapshotIndex
			rf.snapshotMsg = nil
			DPrintf("[S%d][term-%d][%s] applying snapshot at index: %d, term: %d",
				rf.me, rf.currentTerm, rf.state, snapshotMsg.SnapshotIndex, snapshotMsg.SnapshotTerm)
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		firstLogIndex := rf.lastIncludedIndex()
		entries := make([]LogEntry, 0)
		// for rf.commitIndex > rf.lastApplied {
		//     if rf.lastApplied < rf.lastIncludedIndex() {
		//         rf.lastApplied = rf.lastIncludedIndex()
		//     }
		//     if rf.lastApplied >= rf.lastLogEntry().Index {
		//         break
		//     }
		//     rf.lastApplied++
		//     idx := rf.lastApplied - rf.lastIncludedIndex()
		//     entries = append(entries, rf.log[idx])
		// }
		if commitIndex > lastApplied {
			fromIdx := lastApplied - firstLogIndex + 1
			untilIdx := commitIndex - firstLogIndex + 1

			if fromIdx >= 0 && fromIdx < len(rf.log) && untilIdx <= len(rf.log) {
				entries = slices.Clone(rf.log[fromIdx:untilIdx])
			}
		}

		if len(entries) > 0 {
			DPrintf("[S%d][term-%d][%s] applying command till: %v",
				rf.me, rf.currentTerm, rf.state, entries[len(entries)-1])
			rf.lastApplied = entries[len(entries)-1].Index
		}
		rf.mu.Unlock()

		if snapshotMsg != nil {
			rf.applyCh <- *snapshotMsg
		}

		for _, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
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
		log:         []LogEntry{},

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}

	// start with a sentinel entry as raft uses 1-indexed log
	rf.log = append(rf.log, LogEntry{Command: nil, Index: 0, Term: 0})

	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start sender goroutine to wait for majority and apply log to state machine
	go rf.applyMsgSender()

	return rf
}
