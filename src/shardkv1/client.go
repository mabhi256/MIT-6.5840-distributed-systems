package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"slices"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	shardGrpCks     map[tester.Tgid]*shardgrp.Clerk
	shardGrpServers map[tester.Tgid][]string
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:            clnt,
		sck:             sck,
		shardGrpCks:     map[tester.Tgid]*shardgrp.Clerk{},
		shardGrpServers: map[tester.Tgid][]string{},
	}
	return ck
}

func (ck *Clerk) getShardClerk(key string) *shardgrp.Clerk {
	shardID := shardcfg.Key2Shard(key)
	config := ck.sck.Query()

	gid, servers, ok := config.GidServers(shardID)
	if !ok {
		return nil
	}

	cachedServers := ck.shardGrpServers[gid]

	shardCk, ok := ck.shardGrpCks[gid]
	if !ok || !slices.Equal(cachedServers, servers) {
		shardCk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.shardGrpCks[gid] = shardCk
		ck.shardGrpServers[gid] = slices.Clone(servers)
	}

	return shardCk
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		shardCk := ck.getShardClerk(key)
		if shardCk == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		value, version, err := shardCk.Get(key)
		if err != rpc.ErrWrongGroup {
			return value, version, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		shardCk := ck.getShardClerk(key)
		if shardCk == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err := shardCk.Put(key, value, version)
		if err != rpc.ErrWrongGroup {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}
