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

	me uuid.UUID
	// LeaderID int // ID of the Leader in ck.servers
	reqID int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{me: uuid.New(), clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ck.reqID++
	args := rpc.GetArgs{
		Key:      key,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	// DPrintf("[Client-%v] Get(key=%s)\n", ck.me, key)
	deadline := time.Now().Add(500 * time.Millisecond)

	for {
		for _, srvr := range ck.servers {
			reply := rpc.GetReply{}

			ok := ck.clnt.Call(srvr, "KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			// DPrintf("[Client-%v] Get reply from %s: %v\n", ck.me, srvr, reply)

			if reply.Err == rpc.ErrWrongLeader {
				continue
			}

			DPrintf("[Client-%v] Get(key=%s) -> value=%s, version=%d, err=%v\n",
				ck.me, key, reply.Value, reply.Version, reply.Err)
			return reply.Value, reply.Version, reply.Err
		}
		if time.Now().After(deadline) {
			DPrintf("[Client-%v] Get(key=%s) -> DEADLINE\n", ck.me, key)
			return "", 0, rpc.ErrWrongGroup
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

	// DPrintf("[Client-%v] Put(key=%s, value=%s, version=%d)\n", ck.me, key, value, version)
	firstRPC := true
	deadline := time.Now().Add(500 * time.Millisecond)

	for {
		for _, srvr := range ck.servers {
			reply := rpc.PutReply{} // New object for each attempt
			ok := ck.clnt.Call(srvr, "KVServer.Put", &args, &reply)
			if !ok {
				DPrintf("[Client-%v] Put(key=%s) -> RPC FAILED to %s\n", ck.me, key, srvr)
				continue
			}

			if reply.Err == rpc.ErrWrongLeader {
				DPrintf("[Client-%v] Put(key=%s) -> ErrWrongLeader (retrying)\n", ck.me, key)
				continue
			}

			first := firstRPC // Save current state
			firstRPC = false  // Update for next iteration

			if reply.Err == rpc.ErrVersion && !first {
				DPrintf("[Client-%v] Put(key=%s) -> ErrMaybe (version conflict on retry)\n", ck.me, key)
				return rpc.ErrMaybe
			}
			DPrintf("[Client-%v] Put(key=%s) -> err=%v\n", ck.me, key, reply.Err)
			return reply.Err
		}

		if time.Now().After(deadline) {
			DPrintf("[Client-%v] Put(key=%s) -> DEADLINE\n", ck.me, key)
			return rpc.ErrWrongGroup
		}
		time.Sleep(RETRY_FREQ)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	ck.reqID++
	args := shardrpc.FreezeShardArgs{
		Shard:    s,
		Num:      num,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	// DPrintf("[Client-%v] FreezeShard(shard=%d, num=%d)\n", ck.me, s, num)
	for {
		for _, srvr := range ck.servers {
			reply := shardrpc.FreezeShardReply{}

			ok := ck.clnt.Call(srvr, "KVServer.FreezeShard", &args, &reply)
			if !ok {
				continue
			}
			// DPrintf("[Client-%v] Freeze reply from %s: (num:%d)\n", ck.me, srvr, reply.Num)

			if reply.Err == rpc.ErrWrongLeader {
				continue
			}

			DPrintf("[Client-%v] FreezeShard(shard=%d, num=%d) -> state_size=%d, err=%v\n",
				ck.me, s, num, len(reply.State), reply.Err)
			return reply.State, reply.Err
		}
		time.Sleep(RETRY_FREQ)
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

	// DPrintf("[Client-%v] InstallShard(shard=%d, num=%d, state_size=%d)\n", ck.me, s, num, len(state))
	for {
		for _, srvr := range ck.servers {
			reply := shardrpc.InstallShardReply{}

			ok := ck.clnt.Call(srvr, "KVServer.InstallShard", &args, &reply)
			if !ok {
				continue
			}
			// DPrintf("[Client-%v] Install reply from %s: %v\n", ck.me, srvr, reply)

			if reply.Err == rpc.ErrWrongLeader {
				continue
			}

			DPrintf("[Client-%v] InstallShard(shard=%d, num=%d) -> err=%v\n", ck.me, s, num, reply.Err)
			return reply.Err
		}
		time.Sleep(RETRY_FREQ)
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

	// DPrintf("[Client-%v] DeleteShard(shard=%d, num=%d)\n", ck.me, s, num)
	for {
		for _, srvr := range ck.servers {
			reply := shardrpc.DeleteShardReply{}

			ok := ck.clnt.Call(srvr, "KVServer.DeleteShard", &args, &reply)
			if !ok {
				continue
			}
			// DPrintf("[Client-%v] Delete reply from %s: %v\n", ck.me, srvr, reply)

			if reply.Err == rpc.ErrWrongLeader {
				continue
			}

			DPrintf("[Client-%v] DeleteShard(shard=%d, num=%d) -> err=%v\n", ck.me, s, num, reply.Err)
			return reply.Err
		}
		time.Sleep(RETRY_FREQ)
	}
}
