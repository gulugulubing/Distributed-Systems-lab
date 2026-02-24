package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	curCfgVersion rpc.Tversion // 除了开始设置为0，其他都要通过Query得到当前的version
	nxtCfgVersion rpc.Tversion

	// for debug
	id int64
}

func (sck *ShardCtrler) Id() int64 {
	return sck.id
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.id = rand.Int63()
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	currentCfg := sck.Query()
	nextCfg := sck.QueryNext()
	if currentCfg.Num < nextCfg.Num {
		// could any of cfg be nil?
		sck.ChangeConfigTo(nextCfg)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	// init put cfg to kvsrv is guarantee to be OK？
	cfgStr := cfg.String()

	sck.curCfgVersion = 0
	sck.nxtCfgVersion = 0
	sck.Put("cur_config", cfgStr, sck.curCfgVersion)
	sck.Put("nxt_config", cfgStr, sck.nxtCfgVersion)
}

/*
In 5B' and 5C' TEST
When a new ctrl is launched, it will InitController to recover the nxt_config(If not finished)
Then it will get its own cfg(with num plus 1) to process.
*/

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	cfgStr := new.String()
	nxt := sck.QueryNext()

	if new.Num < nxt.Num {
		// is this check really needed? Could this happen?
		time.Sleep(10 * time.Millisecond)
		return
	}

	if new.Num == nxt.Num {
		if !cmpTowCfg(new, nxt) {
			time.Sleep(10 * time.Millisecond)
			return
		}
		// if equals, this could be a recovery and should fall through to proceed
	} else {
		cfgErr := sck.Put("nxt_config", cfgStr, sck.nxtCfgVersion)
		// If Err is errVersion means another ctrl with same Num wins so this one should give up
		// If Err is errMaybe Check again, if equals means previous errMaybe is actually reply.OK so go ahead
		if cfgErr != rpc.OK {
			nxt = sck.QueryNext()
			if !cmpTowCfg(new, nxt) {
				time.Sleep(10 * time.Millisecond)
				return
			}
		}
	}

	old := sck.Query()

	if new.Num <= old.Num {
		// Maybe is redundant here because the server would reject old Cfg
		return
	}

	movingShards := getMovingShards(new, old)

	for _, shard := range movingShards {
		_, originSrvs, _ := old.GidServers(shard)
		originClerk := shardgrp.MakeClerk(sck.clnt, originSrvs)

		var data []byte
		var err rpc.Err
		for {
			data, err = originClerk.FreezeShard(shard, new.Num)
			if err == rpc.ErrUnreachable {
				log.Println("freeze shard", shard, "is unreachable")
				if sck.Query().Num >= new.Num {
					return // Another controller successfully finished the config! Abort.
				}
				// must continue here to get information to determine whether proceed or give up
				// Looping here is Ok, other ctrl would launch to make sck.Query().Num >= new.Num
				continue // Just a network partition, keep trying.
			}
			break
		}
		if err == rpc.ErrWrongGroup || err == rpc.ErrStaleNum {
			continue
		}

		_, destSrvs, _ := new.GidServers(shard)
		destClerk := shardgrp.MakeClerk(sck.clnt, destSrvs)

		for {
			err = destClerk.InstallShard(shard, data, new.Num)
			log.Println("Install shard", shard, "is unreachable")
			if err == rpc.ErrUnreachable {
				if sck.Query().Num >= new.Num {
					return
				}
				continue
			}
			break
		}
		if err == rpc.ErrStaleNum {
			// fall through to DeleteShard
		}

		for {
			err = originClerk.DeleteShard(shard, new.Num)
			log.Println("delete shard", shard, "is unreachable")
			if err == rpc.ErrUnreachable {
				if sck.Query().Num >= new.Num {
					return
				}
				continue
			}
			break
		}
		if err == rpc.ErrStaleNum || err == rpc.ErrWrongGroup {
			// fall through
		}
	}

	putCfgErr := sck.Put("cur_config", cfgStr, sck.curCfgVersion)
	log.Printf("Ctrler %v change cfg%d with reply %v", sck.Id(), new.Num, putCfgErr)

}

func getMovingShards(new *shardcfg.ShardConfig, old *shardcfg.ShardConfig) []shardcfg.Tshid {
	newShards := new.Shards
	oldShards := old.Shards

	shards := make([]shardcfg.Tshid, 0, len(newShards))
	for shard := range newShards {
		if newShards[shard] != oldShards[shard] {
			shards = append(shards, shardcfg.Tshid(shard))
		}
	}
	return shards
}

func cmpTowCfg(cfg1 *shardcfg.ShardConfig, cfg2 *shardcfg.ShardConfig) bool {
	if cfg1.Num != cfg2.Num {
		return false
	}
	if len(cfg1.Groups) != len(cfg2.Groups) {
		return false
	}
	if cfg1.Shards != cfg2.Shards {
		return false
	}
	return true

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, version, err := sck.Get("cur_config")
	if err != rpc.OK {
		return nil
	}
	sck.curCfgVersion = version
	return shardcfg.FromString(cfgStr)
}

func (sck *ShardCtrler) QueryNext() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, version, err := sck.Get("nxt_config")
	if err != rpc.OK {
		return nil
	}
	sck.nxtCfgVersion = version
	return shardcfg.FromString(cfgStr)
}
