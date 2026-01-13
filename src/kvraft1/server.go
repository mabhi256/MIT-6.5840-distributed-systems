package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Record struct {
	Value   string
	Version rpc.Tversion
}

type CachedPut struct {
	PutID int
	Reply rpc.PutReply
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	Store map[string]Record
	Puts  map[uuid.UUID]CachedPut // Map ClientID → cached put (putID + response)
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch req := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		reply := rpc.GetReply{}
		record, exists := kv.Store[req.Key]
		if !exists {
			DPrintf("[S%d] DoOp GET %v, ErrNoKey\n", kv.me, req)
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Err = rpc.OK
		reply.Value = record.Value
		reply.Version = record.Version
		DPrintf("[S%d] DoOp GET %v, reply:%v\n", kv.me, req, reply)
		return reply

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		cached, isCached := kv.Puts[req.ClientID]
		if isCached {
			if req.ReqID < cached.PutID {
				return rpc.PutReply{Err: rpc.ErrVersion}
			}
			if req.ReqID == cached.PutID {
				return cached.Reply
			}
		}

		record, exists := kv.Store[req.Key]
		reply := rpc.PutReply{}

		switch {
		case !exists && req.Version != 0:
			reply.Err = rpc.ErrNoKey

		case !exists:
			// Version: 1 fails in porcupine for some reason
			kv.Store[req.Key] = Record{Value: req.Value, Version: req.Version + 1}
			reply.Err = rpc.OK

		case record.Version != req.Version:
			reply.Err = rpc.ErrVersion

		default:
			kv.Store[req.Key] = Record{Value: req.Value, Version: req.Version + 1}
			reply.Err = rpc.OK
		}

		DPrintf("[S%d] DoOp PUT %v, reply:%v\n", kv.me, req, reply)
		kv.Puts[req.ClientID] = CachedPut{PutID: req.ReqID, Reply: reply}
		return reply

	default:
		log.Fatalf("DoOp should execute only Get & Put and not %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := res.(rpc.GetReply)
	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version

	DPrintf("[S%d] Get args: %v reply: %v\n", kv.me, args, reply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := res.(rpc.PutReply)
	reply.Err = rep.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.Store = map[string]Record{}
	kv.Puts = map[uuid.UUID]CachedPut{}

	return []tester.IService{kv, kv.rsm.Raft()}
}
