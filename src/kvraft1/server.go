package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type Tuple struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me     int
	dead   int32 // set by Kill()
	rsm    *rsm.RSM
	mu     sync.Mutex
	kvData map[string]Tuple
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
		if t, ok := kv.kvData[req.Key]; !ok {
			reply.Err = rpc.ErrNoKey
		} else {
			reply.Value = t.Value
			reply.Version = t.Version
			reply.Err = rpc.OK
		}
		kv.mu.Unlock()
		return reply
	case *rpc.PutArgs:
		var reply rpc.PutReply
		kv.mu.Lock()
		if t, ok := kv.kvData[req.Key]; !ok {
			if req.Version == 0 {
				kv.kvData[req.Key] = Tuple{req.Value, req.Version + 1}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		} else {
			if req.Version != t.Version {
				reply.Err = rpc.ErrVersion
			} else {
				kv.kvData[req.Key] = Tuple{req.Value, req.Version + 1}
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvData)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stores map[string]Tuple
	if d.Decode(&stores) != nil {
		panic("Failed to decode KVServer state from snapshot data")
	}
	kv.kvData = stores
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
	labgob.Register(Tuple{})
	labgob.Register(map[string]Tuple{})

	kv := &KVServer{me: me}
	kv.kvData = make(map[string]Tuple)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
