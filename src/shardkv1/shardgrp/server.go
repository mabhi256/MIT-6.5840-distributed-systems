package shardgrp

import (
	"bytes"
	"log"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

const Debug = false

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

type Record struct {
	Value   string
	Version rpc.Tversion
}

type LastReply struct {
	ReqId int
	Reply any
}

type ShardData struct {
	Frozen bool
	CfgNum shardcfg.Tnum
	Store  map[string]Record
	Cache  map[uuid.UUID]LastReply
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	Shards [shardcfg.NShards]ShardData
}

func (kv *KVServer) DoOp(req any) any {
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(req)
	case rpc.PutArgs:
		return kv.doPut(req)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(req)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(req)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(req)
	default:
		return nil
	}
}

func (kv *KVServer) doGet(req rpc.GetArgs) rpc.GetReply {
	reply := rpc.GetReply{}

	shardID := shardcfg.Key2Shard(req.Key)
	if kv.Shards[shardID].Frozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	cached, exists := kv.Shards[shardID].Cache[req.ClientID]
	if exists && req.ReqID <= cached.ReqId {
		return cached.Reply.(rpc.GetReply)
	}

	record, exists := kv.Shards[shardID].Store[req.Key]
	if !exists {
		DPrintf("[S%d] DoOp GET %v, ErrNoKey\n", kv.me, req)
		reply.Err = rpc.ErrNoKey
		return reply
	}
	// if store is empty and shard is at higher config, then reject
	if len(kv.Shards[shardID].Store) == 0 && kv.Shards[shardID].CfgNum > shardcfg.NumFirst {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	reply.Err = rpc.OK
	reply.Value = record.Value
	reply.Version = record.Version

	kv.Shards[shardID].Cache[req.ClientID] = LastReply{ReqId: req.ReqID, Reply: reply}

	DPrintf("[S%d] DoOp GET %v, reply:%v\n", kv.me, req, reply)
	return reply
}

func (kv *KVServer) doPut(req rpc.PutArgs) rpc.PutReply {
	reply := rpc.PutReply{}

	shardID := shardcfg.Key2Shard(req.Key)
	if kv.Shards[shardID].Frozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	cached, exists := kv.Shards[shardID].Cache[req.ClientID]
	if exists && req.ReqID <= cached.ReqId {
		return cached.Reply.(rpc.PutReply)
	}

	record, exists := kv.Shards[shardID].Store[req.Key]

	switch {
	case !exists && req.Version != 0:
		reply.Err = rpc.ErrNoKey

	case record.Version != req.Version:
		reply.Err = rpc.ErrVersion

	default:
		kv.Shards[shardID].Store[req.Key] = Record{Value: req.Value, Version: req.Version + 1}
		reply.Err = rpc.OK
	}

	kv.Shards[shardID].Cache[req.ClientID] = LastReply{ReqId: req.ReqID, Reply: reply}

	DPrintf("[S%d] DoOp PUT %v, reply:%v\n", kv.me, req, reply)
	return reply
}

func (kv *KVServer) doFreezeShard(req shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	shardData := &kv.Shards[req.Shard]

	cached, exists := shardData.Cache[req.ClientID]
	if exists && req.ReqID <= cached.ReqId {
		return cached.Reply.(shardrpc.FreezeShardReply)
	}

	// First time freezing for this config
	if req.Num > shardData.CfgNum {
		kv.Shards[req.Shard].Frozen = true
		kv.Shards[req.Shard].CfgNum = req.Num
	}

	reply := shardrpc.FreezeShardReply{}
	reply.State = kv.snapshotShard(req.Shard)
	reply.Err = rpc.OK
	reply.Num = kv.Shards[req.Shard].CfgNum

	kv.Shards[req.Shard].Cache[req.ClientID] = LastReply{ReqId: req.ReqID, Reply: reply}

	DPrintf("[S%d] DoOp FreezeShardArgs %v\n", kv.me, req)
	return reply
}

func (kv *KVServer) doInstallShard(req shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	cached, exists := kv.Shards[req.Shard].Cache[req.ClientID]
	if exists && req.ReqID <= cached.ReqId {
		return cached.Reply.(shardrpc.InstallShardReply)
	}

	reply := shardrpc.InstallShardReply{}
	if req.Num > kv.Shards[req.Shard].CfgNum {
		kv.Shards[req.Shard].Frozen = false
		kv.Shards[req.Shard].CfgNum = req.Num

		if req.State != nil {
			kv.restoreShard(req.Shard, req.State)
		}
	}
	reply.Err = rpc.OK
	kv.Shards[req.Shard].Cache[req.ClientID] = LastReply{ReqId: req.ReqID, Reply: reply}

	DPrintf("[S%d] DoOp InstallShardReply %v\n", kv.me, req.ReqID)
	return reply
}

func (kv *KVServer) doDeleteShard(req shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	cached, exists := kv.Shards[req.Shard].Cache[req.ClientID]
	if exists && req.ReqID <= cached.ReqId {
		return cached.Reply.(shardrpc.DeleteShardReply)
	}

	if req.Num >= kv.Shards[req.Shard].CfgNum {
		kv.Shards[req.Shard].CfgNum = req.Num
		kv.Shards[req.Shard].Store = map[string]Record{}
		kv.Shards[req.Shard].Cache = map[uuid.UUID]LastReply{}
		kv.Shards[req.Shard].Frozen = true
	}
	reply := shardrpc.DeleteShardReply{Err: rpc.OK}
	kv.Shards[req.Shard].Cache[req.ClientID] = LastReply{ReqId: req.ReqID, Reply: reply}

	DPrintf("[S%d] DoOp DeleteShardArgs %v\n", kv.me, req)
	return reply
}

func (kv *KVServer) snapshotShard(id shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Shards[id].Store)
	e.Encode(kv.Shards[id].Cache)

	return w.Bytes()
}

func (kv *KVServer) restoreShard(id shardcfg.Tshid, data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var store map[string]Record
	var lastReply map[uuid.UUID]LastReply

	if d.Decode(&store) != nil {
		log.Fatalf("%v couldn't decode store", kv.me)
	}
	if d.Decode(&lastReply) != nil {
		log.Fatalf("%v couldn't decode lastReply", kv.me)
	}

	kv.Shards[id].Store = store
	kv.Shards[id].Cache = lastReply
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Shards)
	DPrintf("[S%d]----- Snapshot : %v\n", kv.me, kv.Shards)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.Shards) != nil {
		log.Fatalf("%v couldn't decode shards", kv.me)
	}
	DPrintf("[S%d]----- Restore : %v\n", kv.me, kv.Shards)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := res.(rpc.GetReply)
	*reply = rep
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := res.(shardrpc.FreezeShardReply)
	*reply = rep
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := res.(shardrpc.InstallShardReply)
	reply.Err = rep.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := res.(shardrpc.DeleteShardReply)
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(LastReply{})

	kv := &KVServer{gid: gid, me: me}

	for i := range shardcfg.NShards {
		kv.Shards[i].Store = make(map[string]Record)
		kv.Shards[i].Cache = map[uuid.UUID]LastReply{}
		kv.Shards[i].CfgNum = shardcfg.NumFirst

		// The first shardgrp (shardcfg.Gid1) owns all shards.
		kv.Shards[i].Frozen = gid != shardcfg.Gid1
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
