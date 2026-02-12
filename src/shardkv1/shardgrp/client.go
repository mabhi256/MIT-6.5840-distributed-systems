package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

const RETRY_FREQ = 100 * time.Millisecond

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader int //servers[i], last leader
	me     uuid.UUID
	reqID  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0, me: uuid.New()}
	return ck
}

func (ck *Clerk) callGet(args *rpc.GetArgs) (string, rpc.Tversion, rpc.Err) {
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		for i := range ck.servers {
			server := (ck.leader + i) % len(ck.servers)

			reply := rpc.GetReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", args, &reply)
			if !ok {
				continue
			}
			// DPrintf("[Client-%v] Get reply from %s: %v\n", ck.me, srvr, reply)

			if reply.Err == rpc.ErrWrongLeader {
				continue
			}

			DPrintf("[Client-%v] Get(key=%s) -> value=%s, version=%d, err=%v\n",
				ck.me, args.Key, reply.Value, reply.Version, reply.Err)
			return reply.Value, reply.Version, reply.Err
		}
		if time.Now().After(deadline) {
			DPrintf("[Client-%v] Get(key=%s) -> DEADLINE\n", ck.me, args.Key)
			return "", 0, rpc.ErrWrongGroup
		}
		time.Sleep(RETRY_FREQ)
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ck.reqID++
	args := rpc.GetArgs{
		Key:      key,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}
	return ck.callGet(&args)
}

func (ck *Clerk) DoGet(args *rpc.GetArgs) (string, rpc.Tversion, rpc.Err) {
	return ck.callGet(args)
}

func (ck *Clerk) callPut(args *rpc.PutArgs) rpc.Err {
	firstRPC := true
	deadline := time.Now().Add(500 * time.Millisecond)

	for {
		for i := range ck.servers {
			server := (ck.leader + i) % len(ck.servers)

			reply := rpc.PutReply{} // New object for each attempt
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", args, &reply)
			if !ok {
				DPrintf("[Client-%v] Put(key=%s) -> RPC FAILED to %s\n", ck.me, args.Key, server)
				continue
			}

			if reply.Err == rpc.ErrWrongLeader {
				DPrintf("[Client-%v] Put(key=%s) -> ErrWrongLeader (retrying)\n", ck.me, args.Key)
				continue
			}

			first := firstRPC // Save current state
			firstRPC = false  // Update for next iteration

			if reply.Err == rpc.ErrVersion && !first {
				DPrintf("[Client-%v] Put(key=%s) -> ErrMaybe (version conflict on retry)\n", ck.me, args.Key)
				return rpc.ErrMaybe
			}
			DPrintf("[Client-%v] Put(key=%s) -> err=%v\n", ck.me, args.Key, reply.Err)
			return reply.Err
		}

		if time.Now().After(deadline) {
			DPrintf("[Client-%v] Put(key=%s) -> DEADLINE\n", ck.me, args.Key)
			return rpc.ErrWrongGroup
		}
		time.Sleep(RETRY_FREQ)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	ck.reqID++
	args := rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}
	return ck.callPut(&args)
}

func (ck *Clerk) DoPut(args *rpc.PutArgs) rpc.Err {
	return ck.callPut(args)
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	ck.reqID++
	args := shardrpc.FreezeShardArgs{
		Shard:    s,
		Num:      num,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.FreezeShardReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK {
				ck.leader = server
				return reply.State, reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return nil, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	ck.reqID++
	args := shardrpc.InstallShardArgs{
		Shard:    s,
		State:    state,
		Num:      num,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.InstallShardReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK {
				ck.leader = server
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	ck.reqID++
	args := shardrpc.DeleteShardArgs{
		Shard:    s,
		Num:      num,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			reply := shardrpc.DeleteShardReply{}
			ok := ck.clnt.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				continue
			}
			if reply.Err == rpc.OK {
				ck.leader = server
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
