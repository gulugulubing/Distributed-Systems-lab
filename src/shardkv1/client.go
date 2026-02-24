package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.

	cfg shardcfg.ShardConfig
	// Tgid->*shardgrp.Clerk, mapping here is constant
	// Removed gid may still stay here, but it's OK
	grpClerks map[tester.Tgid]*shardgrp.Clerk
	mu        sync.Mutex
}

// key->shard->(gid, servers)->grpClerk
// mapping of gid and servers is constant
func (ck *Clerk) getGrpClerk(key string) *shardgrp.Clerk {
	shard := shardcfg.Key2Shard(key)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	gid, servers, _ := ck.cfg.GidServers(shard)
	grpClerk, ok := ck.grpClerks[gid]
	if !ok {
		// Group servers created at first time
		grpClerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.grpClerks[gid] = grpClerk
	}
	//fmt.Println("grpClerk gid:", gid)
	return grpClerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,

		grpClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		cfg := ck.sck.Query()
		ck.mu.Lock()
		ck.cfg = *cfg
		ck.mu.Unlock()

		gck := ck.getGrpClerk(key)
		value, version, err := gck.Get(key)
		if err == rpc.ErrWrongGroup || err == rpc.ErrShardFrozen || err == rpc.ErrUnreachable {
			time.Sleep(50 * time.Millisecond)
			fmt.Println("shardServer, get err:, key", err, key)
			continue
		}
		return value, version, err
	}

}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	retry := false
	for {
		cfg := ck.sck.Query()
		ck.mu.Lock()
		ck.cfg = *cfg
		ck.mu.Unlock()

		gck := ck.getGrpClerk(key)
		err := gck.Put(key, value, version)
		if err == rpc.ErrWrongGroup || err == rpc.ErrShardFrozen || err == rpc.ErrUnreachable {
			time.Sleep(50 * time.Millisecond)
			fmt.Println("shardServer, put err:, key", err, key)
			retry = true
			continue
		}
		if retry && err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		return err
	}
}
