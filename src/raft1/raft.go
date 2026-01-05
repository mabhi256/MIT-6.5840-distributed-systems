package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

// first index is 1 ?
type LogEntry struct {
	Command any
	Index   int
	Term    int // term when entry was received by leader
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	state           State
	lastHeartbeat   time.Time
	electionTimeout int

	// Persistent state on ALL servers: (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int // peer id
	log         []LogEntry

	// Volatile state on ALL servers
	commitIndex int
	lastApplied int

	// Volatile state on Leader: (Reinitialized after election)
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = ELECTION_TIMEOUT_MIN + rand.IntN(ELECTION_TIMEOUT_RANGE)
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	// Reinitialize nextIndex and matchIndex
	lastLogIndex := rf.log[len(rf.log)-1].Index
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastLogIndex

	go rf.heartbeatToAll(rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
}

func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	if isLeader {
		rf.mu.Lock()
		index = len(rf.log)
		latestEntry := LogEntry{Command: command, Index: index, Term: term}
		rf.log = append(rf.log, latestEntry)
		rf.matchIndex[rf.me] = index
		rf.mu.Unlock()

		rf.replicateToAll(term)
	}

	return index, term, isLeader
}

func (rf *Raft) replicateToAll(currentTerm int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		request := rf.prepareAppendEntries(i, currentTerm)

		go rf.sendAppendEntries(i, request, false)
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

// Check if a leader election should be started.
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		timeoutDuration := time.Duration(rf.electionTimeout) * time.Millisecond

		if rf.state != Leader && time.Since(rf.lastHeartbeat) > timeoutDuration {
			go rf.startElection(timeoutDuration)
		}

		time.Sleep(timeoutDuration)
	}
}

func (rf *Raft) startElection(timeoutDuration time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	ch := make(chan bool, len(rf.peers))

	rf.mu.Lock()
	rf.becomeCandidate()
	currentTerm := rf.currentTerm
	lastLogEntry := rf.log[len(rf.log)-1]
	rf.mu.Unlock()

	request := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}

	// DPrintf("[S%d][term-%d] sending RequestVote RPCs", rf.me, currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendRequestVote(i, request, ch)
	}

	rf.tallyVotes(ctx, ch, currentTerm)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan bool) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	ch <- ok && reply.VoteGranted

	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.becomeFollower(reply.Term)
		rf.mu.Unlock()
	}
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
					// DPrintf("[S%d][term-%d] got majority", rf.me, rf.currentTerm)
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) heartbeatToAll(currentTerm int) {
	for !rf.killed() {
		// If the term changed then stop sending Heartbeats
		if rf.currentTerm != currentTerm {
			return
		}

		// DPrintf("[S%d][term-%d] sending Heartbeat to all followers", rf.me, currentTerm)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			request := rf.prepareAppendEntries(i, currentTerm)
			rf.mu.Unlock()

			// piggyback log replication on heartbeats to reduce RPC overhead
			go rf.sendAppendEntries(i, request, true)
		}

		time.Sleep(HEARTBEAT_FREQ)
	}
}

func (rf *Raft) prepareAppendEntries(server int, currentTerm int) *AppendEntriesArgs {
	nextIndex := rf.nextIndex[server]
	prevLogEntry := rf.log[nextIndex-1]
	entries := slices.Clone(rf.log[nextIndex:])
	commitIndex := rf.commitIndex

	request := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogEntry.Index,
		PrevLogTerm:  prevLogEntry.Term,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	return request
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, isHeartbeat bool) {
	reply := &AppendEntriesReply{}

	for !rf.killed() {
		if rf.currentTerm != args.Term {
			return
		}

		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			if isHeartbeat {
				return
			}
			// If sending/receving RPC failed, retry after sleep
			time.Sleep(RETRY_FREQ)
			continue
		}

		if reply.Term > args.Term {
			rf.mu.Lock()
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.mu.Lock()
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// Try to advance commitIndex
			rf.updateCommitIndex()
			rf.mu.Unlock()

			return
		} else {
			// log starts with a sentinel entry at index 0 (which should never be replicated)
			rf.mu.Lock()
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			rf.mu.Unlock()

			if isHeartbeat {
				return
			}
			// If sending/receving RPC failed, retry after sleep
			time.Sleep(RETRY_FREQ)
			continue
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N s.t.
	// N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
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
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		var msgs []raftapi.ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()

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
		log:         []LogEntry{},

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}

	// start with a sentinel entry as raft uses 1-indexed log
	rf.log = append(rf.log, LogEntry{Command: nil, Index: 0, Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start sender goroutine to wait for majority and apply log to state machine
	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applyMsgSender()

	// start ticker goroutine to start elections
	rf.resetElectionTimeout()
	go rf.electionTicker()

	return rf
}
