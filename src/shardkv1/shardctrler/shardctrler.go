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
	for {
		old, _, err := sck.getCurrent()
		if err != rpc.OK {
			return
		}
		new, _, err := sck.getNext()
		if err != rpc.OK {
			return
		}
		if new == nil || new.Num <= old.Num {
			return // Already caught up
		}
		// recover movement of shards
		sck.ChangeConfigTo(new)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.IKVClerk.Put("config/current", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		current, currVer, _ := sck.getCurrent()

		// current config is up-to-date
		if current.Num >= new.Num {
			return
		}

		_, nextVer, _ := sck.getNext()
		if err := sck.saveNext(new, nextVer); err != rpc.OK {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		if !sck.moveShards(current, new) {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		if err := sck.saveCurrent(new, currVer); err != rpc.OK {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		sck.clearNext(nextVer + 1)
		return
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	curr, _, _ := sck.getCurrent()
	return curr
}

func (sck *ShardCtrler) getConfig(key string) (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	cfgStr, ver, err := sck.IKVClerk.Get(key)
	if err == rpc.ErrNoKey || cfgStr == "" {
		return nil, ver, rpc.OK
	}
	if err != rpc.OK {
		return nil, ver, err
	}
	return shardcfg.FromString(cfgStr), ver, err
}

func (sck *ShardCtrler) getCurrent() (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	return sck.getConfig("config/current")
}

func (sck *ShardCtrler) getNext() (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	return sck.getConfig("config/next")
}

func (sck *ShardCtrler) setConfig(key, value string, ver rpc.Tversion) rpc.Err {
	err := sck.Put(key, value, ver)

	if err == rpc.ErrMaybe {
		val, _, err := sck.Get(key)
		if err != rpc.OK {
			return err
		}
		if val == value {
			return rpc.OK
		}
		return rpc.ErrVersion
	}
	return err
}

func (sck *ShardCtrler) saveCurrent(cfg *shardcfg.ShardConfig, ver rpc.Tversion) rpc.Err {
	return sck.setConfig("config/current", cfg.String(), ver)
}

func (sck *ShardCtrler) saveNext(cfg *shardcfg.ShardConfig, ver rpc.Tversion) rpc.Err {
	return sck.setConfig("config/next", cfg.String(), ver)
}

func (sck *ShardCtrler) clearNext(ver rpc.Tversion) {
	sck.setConfig("config/next", "", ver)
}

func (sck *ShardCtrler) moveShards(old, new *shardcfg.ShardConfig) bool {
	for i := range shardcfg.NShards {
		shardId := shardcfg.Tshid(i)
		oldGid, oldServers, _ := old.GidServers(shardId)
		newGid, newServers, _ := new.GidServers(shardId)
		if oldGid == newGid {
			continue
		}

		oldClerk := shardgrp.MakeClerk(sck.clnt, oldServers)
		shardState, err := oldClerk.FreezeShard(shardId, new.Num)
		if err != rpc.OK {
			return false
		}

		newClerk := shardgrp.MakeClerk(sck.clnt, newServers)
		err = newClerk.InstallShard(shardId, shardState, new.Num)
		if err != rpc.OK {
			return false
		}

		err = oldClerk.DeleteShard(shardId, new.Num)
		if err != rpc.OK {
			return false
		}
	}
	return true
}
