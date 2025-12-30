package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type tuple struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	mu   sync.Mutex
	data map[string]tuple
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case *rpc.GetArgs:
		var reply rpc.GetReply
		kv.mu.Lock()
		if t, ok := kv.data[req.Key]; !ok {
			reply.Err = rpc.ErrNoKey
		} else {
			reply.Value = t.value
			reply.Version = t.version
			reply.Err = rpc.OK
		}
		kv.mu.Unlock()
		return reply
	case *rpc.PutArgs:
		var reply rpc.PutReply
		kv.mu.Lock()
		if t, ok := kv.data[req.Key]; !ok {
			if req.Version == 0 {
				kv.data[req.Key] = tuple{req.Value, req.Version + 1}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		} else {
			if req.Version != t.version {
				reply.Err = rpc.ErrVersion
			} else {
				kv.data[req.Key] = tuple{req.Value, req.Version + 1}
				reply.Err = rpc.OK
			}
		}
		kv.mu.Unlock()
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(args)
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
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
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	// Must Register pointer type here, not value type!!!
	// Otherwise, rpc between raft peers will Not serialize/deserialize it correctly
	// command in Raft's log is an interface, its type must be determined at runtime
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.data = make(map[string]tuple)
	return []tester.IService{kv, kv.rsm.Raft()}
}
