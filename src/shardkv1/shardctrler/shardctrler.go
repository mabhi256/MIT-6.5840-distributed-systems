package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.IKVClerk.Put("config", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		old := sck.Query()
		if old.Num >= new.Num {
			return
		}

		success := true
		for i := range shardcfg.NShards {
			shardId := shardcfg.Tshid(i)
			oldGid, oldServers, oldExists := old.GidServers(shardId)
			newGid, newServers, newExists := new.GidServers(shardId)
			if !oldExists || !newExists {
				success = false
				break
			}
			if oldGid == newGid {
				continue
			}

			oldClerk := shardgrp.MakeClerk(sck.clnt, oldServers)
			shardState, err := oldClerk.FreezeShard(shardId, new.Num)
			if err != rpc.OK {
				success = false
				break
			}

			newClerk := shardgrp.MakeClerk(sck.clnt, newServers)
			err = newClerk.InstallShard(shardId, shardState, new.Num)
			if err != rpc.OK {
				success = false
				break
			}

			err = oldClerk.DeleteShard(shardId, new.Num)
			if err != rpc.OK {
				success = false
				break
			}
		}

		if success {
			sck.IKVClerk.Put("config", new.String(), rpc.Tversion(old.Num))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	cfgStr, _, _ := sck.IKVClerk.Get("config")
	return shardcfg.FromString(cfgStr)
}
