package rsm

import (
	"context"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Me  int
	Id  uuid.UUID
	Req any
}

type OpRes struct {
	Id  uuid.UUID
	Res any
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

	resChMap map[int]chan OpRes

	ctx    context.Context
	cancel context.CancelFunc
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
	ctx, cancel := context.WithCancel(context.Background())
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		resChMap:     map[int]chan OpRes{},
		ctx:          ctx,
		cancel:       cancel,
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

func (rsm *RSM) reader() {
	defer rsm.cancel()

	for {
		msg := <-rsm.applyCh

		switch {
		case msg.CommandValid:
			op := msg.Command.(Op)
			DPrintf("[S%d] Op: %v\n", rsm.me, op)

			res := rsm.sm.DoOp(op.Req)
			DPrintf("[S%d] DoOp result: %v\n", rsm.me, res)

			rsm.mu.Lock()
			ch, exists := rsm.resChMap[msg.CommandIndex]
			_, isLeader := rsm.rf.GetState()
			rsm.mu.Unlock()

			if isLeader && exists && ch != nil {
				opRes := OpRes{Id: op.Id, Res: res}
				ch <- opRes
			}
		default:
			return
		}
	}
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	opId := uuid.New()
	op := Op{Me: rsm.me, Id: opId, Req: req}

	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan OpRes)
	rsm.resChMap[index] = ch
	rsm.mu.Unlock()

	defer func() {
		close(ch)

		rsm.mu.Lock()
		delete(rsm.resChMap, index)
		rsm.mu.Unlock()
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case opRes := <-ch:
			rsm.mu.Lock()
			term2, isLeader2 := rsm.rf.GetState()
			rsm.mu.Unlock()
			if term != term2 || !isLeader2 || op.Id != opRes.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, opRes.Res

		case <-ticker.C:
			rsm.mu.Lock()
			term2, isLeader2 := rsm.rf.GetState()
			rsm.mu.Unlock()
			if term != term2 || !isLeader2 {
				return rpc.ErrWrongLeader, nil
			}

		case <-rsm.ctx.Done():
			return rpc.ErrWrongLeader, nil

			// case <-time.After(1 * time.Second):
			// 	DPrintf("-----1s")
			// 	return rpc.ErrWrongLeader, nil
		}
	}
}
