package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Id  uint64
	Req any
}

type PendingOp struct {
	id      uint64
	term    int
	replyCh chan any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	nextId      uint64
	pending     map[int]*PendingOp
	lastApplied int
	dead        atomic.Int32
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		nextId:       1,
		pending:      map[int]*PendingOp{},
	}

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()

	// Check if already shut down
	if rsm.dead.Load() == 1 {
		return rpc.ErrWrongLeader, nil
	}

	op := Op{Id: rsm.nextId, Req: req}
	rsm.nextId++

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	rsm.pending[index] = &PendingOp{id: op.Id, term: term, replyCh: ch}
	rsm.mu.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case result := <-ch:
			if result == nil {
				return rpc.ErrWrongLeader, nil
			} else {
				return rpc.OK, result
			}

		case <-ticker.C:
			// Check for shutdown on each tick
			if rsm.dead.Load() == 1 {
				rsm.mu.Lock()
				delete(rsm.pending, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}

			term2, isLeader2 := rsm.rf.GetState()
			if term != term2 || !isLeader2 {
				rsm.mu.Lock()
				delete(rsm.pending, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		switch {
		case msg.CommandValid:
			rsm.applyCommand(msg)
		case msg.SnapshotValid:
			rsm.applySnapshot(msg)
		}
	}

	// Mark as dead when applyCh closes
	rsm.dead.Store(1)

	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// Clean up all pending operations on shutdown
	for index := range rsm.pending {
		rsm.pending[index].replyCh <- nil
		delete(rsm.pending, index)
	}
}

func (rsm *RSM) applyCommand(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if msg.CommandIndex <= rsm.lastApplied {
		return
	}
	rsm.lastApplied = msg.CommandIndex

	op := msg.Command.(Op)
	res := rsm.sm.DoOp(op.Req)

	if pending, exists := rsm.pending[msg.CommandIndex]; exists {
		term, isLeader := rsm.rf.GetState()
		if isLeader && term == pending.term && op.Id == pending.id {
			pending.replyCh <- res
		} else {
			pending.replyCh <- nil
		}
		delete(rsm.pending, msg.CommandIndex)
	}

	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
		rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
		DPrintf("[S%d] Snapshot @ Index-%d\n", rsm.me, msg.CommandIndex)
	}
}

func (rsm *RSM) applySnapshot(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if msg.SnapshotIndex <= rsm.lastApplied {
		return
	}
	rsm.sm.Restore(msg.Snapshot)
	rsm.lastApplied = msg.SnapshotIndex

	// Clean up stale pending operations
	for index, pending := range rsm.pending {
		if index <= msg.SnapshotIndex {
			pending.replyCh <- nil
			delete(rsm.pending, index)
		}
	}
}
