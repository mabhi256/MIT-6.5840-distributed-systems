package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

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
}

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

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // idx in peers[]
	LastLogIndex int // last index in log[]
	LastLogTerm  int // term value in the last entry of log[]
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[S%d][term-%d][%s] received RequestVote from S%d for term %d\n", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		log.Printf("    My Term is low, becoming follower")
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
		log.Printf("    granting vote")
		rf.lastHeartbeat = time.Now()
		rf.votedFor = args.CandidateId
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
	Term     int // Leaders's term
	LeaderId int // idx in peers[]

	// index of log entry immediately preceding the LogEntry in Entries[0],
	// if no value in entries then send the last index/term
	PrevLogIndex int
	// term value in the last entry of log[]
	PrevLogTerm int

	Entries      []LogEntry // empty for heartbeat; may send more than one for efficiency
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // actual current term, may need to update self's term
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler.Received by follower, to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[S%d][term-%d][%s] received AppendEntry from S%d for term %d\n", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term)

	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// // If we're a candidate, and receive AppendEntries with same term
	// // it means a leader has been elected - step down
	// if args.Term == rf.currentTerm && rf.state == Candidate {
	// 	log.Printf("    Another leader exists, stepping down to follower")
	// 	rf.becomeFollower(args.Term)
	// } else
	if args.Term > rf.currentTerm {
		log.Printf("    My Term is low, becoming follower")
		rf.becomeFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	if len(rf.log) < args.PrevLogIndex {
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if args.PrevLogIndex > 0 {
		if len(rf.log) < args.PrevLogIndex {
			reply.Success = false
			return
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.log = rf.log[:args.PrevLogIndex]
			reply.Success = false
			return
		}
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last log entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
}

// Invoked by leader
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// term, isLeader := rf.GetState()

	// if isLeader {
	// 	// TODO: do the stuff
	// }

	// Your code here (3B).

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
		electionTimeout := 500 + rand.IntN(100)
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
	majority := (len(rf.peers) + 1) / 2
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

	log.Printf("[S%d][term-%d] sending RequestVote RPCs", rf.me, currentTerm)
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
				log.Printf("[S%d][term-%d] got vote from S%d", rf.me, currentTerm, i)
				ch <- true
			} else {
				ch <- false
				if reply.Term > rf.currentTerm {
					log.Printf("    My Term is low, becoming follower")
					rf.becomeFollower(reply.Term)
				}
			}
			rf.mu.Unlock()
		}()
	}

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
					log.Printf("[S%d][term-%d] got majority", rf.me, rf.currentTerm)
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
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader

	go rf.startReplication()
}

const HEARTBEAT_FREQ = 100 * time.Millisecond

func (rf *Raft) startReplication() {
	/**
	  All Servers:
	  • If commitIndex > lastApplied: increment lastApplied, apply
	  log[lastApplied] to state machine (§5.3)

	  Leaders:
	  • If command received from client: append entry to local log,
	  respond after entry applied to state machine (§5.3)
	  • If last log index ≥ nextIndex for a follower: send
	  AppendEntries RPC with log entries starting at nextIndex
	  • If successful: update nextIndex and matchIndex for
	  follower (§5.3)
	  • If AppendEntries fails because of log inconsistency:
	  decrement nextIndex and retry (§5.3)
	  • If there exists an N such that N > commitIndex, a majority
	  of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	  set commitIndex = N
	*/

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	// lastApplied := rf.lastApplied
	// nextIndex := rf.nextIndex
	// matchIndex := rf.matchIndex
	rf.mu.Unlock()

	// 10 hearbeat per sec
	for !rf.killed() {
		rf.mu.Lock()
		// If we are no longer the leader, or the term changed, then stop sending AppendEntries RPC
		if rf.state != Leader || currentTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_FREQ)

		log.Printf("[S%d][term-%d] sending AppendEntries RPCs", rf.me, currentTerm)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func() {
				// NO Logs for now, only send hearbeats
				request := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					Entries:      []LogEntry{},
					LeaderCommit: commitIndex,
				}
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(i, request, reply)
				if !ok {
					return
				}

				// Don't hold locks when sending RPC
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > currentTerm {
					log.Printf("    My Term is low, becoming follower")
					rf.becomeFollower(reply.Term)
					return
				}
			}()
		}
	}
}

func (rf *Raft) getLastLogEntry() LogEntry {
	idx := len(rf.log) - 1
	if idx > 0 {
		return rf.log[idx]
	}

	return LogEntry{Index: -1, Term: -1}
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

		nextIndex:  []int{},
		matchIndex: []int{},
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
