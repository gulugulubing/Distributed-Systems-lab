package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader = 0
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	for {
		// should assign new reply to avoid old field value in retry loop
		// if not, labgob warning: Decoding into a non-default variable/field Err may not work
		// when means if new Err is "", it will NOT overwrite the old value
		reply := rpc.GetReply{}
		if ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
			//log.Println("call KVServer.Get fail") // just print, keep going
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// ErrNoKey or OK
		return reply.Value, reply.Version, reply.Err
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for {
		// should assign new reply to avoid old field value in retry loop
		// if not, labgob warning: Decoding into a non-default variable/field Err may not work
		reply := rpc.PutReply{}
		if ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply); !ok || reply.Err == rpc.ErrWrongLeader {
			// log.Println("call KVServer.Put fail") // just print, keep going
			retried = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
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
