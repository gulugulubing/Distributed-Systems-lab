package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	timeout := time.NewTimer(1 * time.Second)
	for {
		reply := rpc.GetReply{}
		select {
		// should assign new reply to avoid old field value in retry loop
		// if not, labgob warning: Decoding into a non-default variable/field Err may not work
		// when means if new Err is "", it will NOT overwrite the old value
		case <-timeout.C:
			reply.Err = rpc.ErrUnreachable
			return reply.Value, reply.Version, reply.Err
		default:
			if ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.Get", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
				//log.Println("call KVServer.Get fail") // just print, keep going
				ck.leader = (ck.leader + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			// ErrNoKey or OK
			return reply.Value, reply.Version, reply.Err
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	timeout := time.NewTimer(1 * time.Second)
	for {
		reply := rpc.GetReply{}
		select {
		case <-timeout.C:
			reply.Err = rpc.ErrUnreachable
			return reply.Err
		default:
			//
			//fmt.Println("server's clerk is putting", key, value, version)
			// should assign new reply to avoid old field value in retry loop
			// if not, labgob warning: Decoding into a non-default variable/field Err may not work
			reply := rpc.PutReply{}
			if ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.Put", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
				//fmt.Println("server's clerk is retrying putting because:", ok, reply.Err)
				retried = true
				ck.leader = (ck.leader + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			switch reply.Err {
			case rpc.ErrVersion:
				if !retried {
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			default:
				// rpc.OK or rpc.ErrNoKey
				return reply.Err
			}
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Num: num, Shard: s}
	timeout := time.NewTimer(1 * time.Second)
	for {
		reply := shardrpc.FreezeShardReply{}
		select {
		case <-timeout.C:
			reply.Err = rpc.ErrUnreachable
			return reply.State, reply.Err
		default:
			if ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.FreezeShard", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
				//fmt.Println("retry freezeShard rpc because reply", ok, reply.Err)
				ck.leader = (ck.leader + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			return reply.State, reply.Err
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	timeout := time.NewTimer(1 * time.Second)
	for {
		reply := shardrpc.InstallShardReply{}
		select {
		case <-timeout.C:
			reply.Err = rpc.ErrUnreachable
			return reply.Err
		default:
			if ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.InstallShard", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
				//fmt.Println("retry installShard rpc because reply", ok, reply.Err)
				ck.leader = (ck.leader + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return reply.Err
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	timeout := time.NewTimer(1 * time.Second)
	for {
		reply := shardrpc.DeleteShardReply{}
		select {
		case <-timeout.C:
			reply.Err = rpc.ErrUnreachable
			return reply.Err
		default:
			if ok := ck.clnt.Call(ck.servers[ck.leader], "ShardGroup.DeleteShard", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
				//if !ok {
				//fmt.Println("retry deleteShard rpc because reply", ok, reply.Err)
				//}
				ck.leader = (ck.leader + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return reply.Err
		}
	}
}
