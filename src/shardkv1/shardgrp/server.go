package shardgrp

import (
	"bytes"
	"maps"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Tuple struct {
	Value   string
	Version rpc.Tversion
}

type ShardGroup struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	shards [shardcfg.NShards]*Shard
}

type Shard struct {
	mu         sync.RWMutex
	ShardState // shard could access Data directly
}

// An individual state is easy to serilialize and send in rpc
type ShardState struct {
	Data   map[string]Tuple
	Frozen bool
	Owned  bool
	CfgNum shardcfg.Tnum
}

func (grp *ShardGroup) DoOp(req any) any {
	switch req := req.(type) {
	case *rpc.GetArgs:
		return grp.shards[shardcfg.Key2Shard(req.Key)].doGet(req)
	case *rpc.PutArgs:
		return grp.shards[shardcfg.Key2Shard(req.Key)].doPut(req)
	case *shardrpc.FreezeShardArgs:
		//fmt.Println("doFreezeShard: ", req.Shard)
		return grp.shards[req.Shard].doFreeze(req)
	case *shardrpc.InstallShardArgs:
		//fmt.Println("doInstallShard: ", req.Shard)
		return grp.shards[req.Shard].doInstall(req)
	case *shardrpc.DeleteShardArgs:
		//fmt.Println("doDeleteShard: ", req.Shard)
		return grp.shards[req.Shard].doDelete(req)
	}
	return nil
}

func (sh *Shard) doPut(req *rpc.PutArgs) (reply rpc.PutReply) {

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if !sh.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	if sh.Frozen {
		reply.Err = rpc.ErrShardFrozen
		return reply
	}

	if t, ok := sh.Data[req.Key]; !ok {
		if req.Version == 0 {
			sh.Data[req.Key] = Tuple{req.Value, req.Version + 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if req.Version != t.Version {
			reply.Err = rpc.ErrVersion
		} else {
			sh.Data[req.Key] = Tuple{req.Value, req.Version + 1}
			reply.Err = rpc.OK
		}
	}
	return reply
}

func (sh *Shard) doGet(req *rpc.GetArgs) (reply rpc.GetReply) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if !sh.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	if t, ok := sh.Data[req.Key]; !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = t.Value
		reply.Version = t.Version
		reply.Err = rpc.OK
	}
	return reply
}

func (sh *Shard) doFreeze(req *shardrpc.FreezeShardArgs) (reply shardrpc.FreezeShardReply) {

	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.CfgNum > req.Num {
		reply.Num = sh.CfgNum
		reply.Err = rpc.ErrStaleNum
		return reply
	}

	if !sh.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	sh.Frozen = true
	sh.CfgNum = req.Num

	reply.Err = rpc.OK
	reply.Num = sh.CfgNum

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(sh.Data)
	reply.State = w.Bytes()

	return reply
}

func (sh *Shard) doInstall(req *shardrpc.InstallShardArgs) (reply shardrpc.InstallShardReply) {

	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.CfgNum > req.Num {
		reply.Err = rpc.ErrStaleNum
		return reply
	}
	sh.CfgNum = req.Num
	r := bytes.NewBuffer(req.State)
	d := labgob.NewDecoder(r)
	var state map[string]Tuple
	if d.Decode(&state) != nil {
		panic("Failed to decode KVServer state from doInstall data")
	}
	sh.Owned = true
	sh.Data = state
	sh.Frozen = false
	reply.Err = rpc.OK
	return reply
}

func (sh *Shard) doDelete(req *shardrpc.DeleteShardArgs) (reply shardrpc.DeleteShardReply) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.CfgNum > req.Num {
		reply.Err = rpc.ErrStaleNum
		return reply
	}
	sh.CfgNum = req.Num
	if !sh.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	sh.Frozen = false
	sh.Owned = false
	sh.Data = make(map[string]Tuple)
	// fmt.Println("after delete, sh data size len(sh.Data): ", len(sh.Data))
	reply.Err = rpc.OK
	return reply
}

func (grp *ShardGroup) Snapshot() []byte {
	// Your code here
	states := make([]ShardState, shardcfg.NShards)
	for i := 0; i < shardcfg.NShards; i++ {
		shard := grp.shards[i]
		shard.mu.RLock()
		state := shard.ShardState
		state.Data = maps.Clone(shard.Data)
		states[i] = state
		shard.mu.RUnlock()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(states)
	// fmt.Printf("snapshot grp.gid:%d, grp.me: %d \n", grp.gid, grp.me)
	return w.Bytes()
}

func (grp *ShardGroup) Restore(data []byte) {
	// Your code here
	// No need grp's mu here，although read grp.shards[i] it's constant(read only)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var states []ShardState
	if d.Decode(&states) != nil {
		panic("Failed to decode KVServer state from snapshot data")
	}
	for i := 0; i < shardcfg.NShards; i++ {
		shard := grp.shards[i]
		shard.mu.Lock()
		shard.ShardState = states[i]
		shard.mu.Unlock()
	}
}

func (grp *ShardGroup) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, rep := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (grp *ShardGroup) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, rep := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	} else {
		reply.Err = err
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (grp *ShardGroup) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, rep := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(shardrpc.FreezeShardReply)
	} else {
		reply.Err = err
	}
}

// Install the supplied state for the specified shard.
func (grp *ShardGroup) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, rep := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(shardrpc.InstallShardReply)
	} else {
		reply.Err = err
	}
}

// Delete the specified shard.
func (grp *ShardGroup) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := grp.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(shardrpc.DeleteShardReply)
	} else {
		reply.Err = err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (grp *ShardGroup) Kill() {
	atomic.StoreInt32(&grp.dead, 1)
	// Your code here, if desired.
}

func (grp *ShardGroup) killed() bool {
	z := atomic.LoadInt32(&grp.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})
	labgob.Register(&shardrpc.FreezeShardArgs{})
	labgob.Register(&shardrpc.InstallShardArgs{})
	labgob.Register(&shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	labgob.Register(Tuple{})
	labgob.Register(ShardState{})
	labgob.Register(map[string]Tuple{})

	shardGrp := &ShardGroup{gid: gid, me: me}
	for i := 0; i < shardcfg.NShards; i++ {
		shardGrp.shards[i] = &Shard{
			ShardState: ShardState{
				Data:   make(map[string]Tuple),
				Frozen: false,
				Owned:  gid == shardcfg.Gid1,
				CfgNum: 0,
			},
		}
	}

	shardGrp.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, shardGrp)

	// Your code here
	return []tester.IService{shardGrp, shardGrp.rsm.Raft()}
}
