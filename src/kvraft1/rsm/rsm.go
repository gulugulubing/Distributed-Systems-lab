package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

type PendingOp struct {
	id   int64
	term int
	ch   chan any
}

func (pendingOp *PendingOp) notify(res any) {
	// Best practice for avoiding blocking
	select {
	case pendingOp.ch <- res:
	default:
	}
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	pendingOps map[int]*PendingOp //logIdx : opId

	shutdownCh chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendingOps:   make(map[int]*PendingOp),
		shutdownCh:   make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 && maxraftstate >= 0 {
		sm.Restore(snapshot)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	op := Op{Me: rsm.me, Id: rand.Int63(), Req: req}
	rsm.mu.Lock()
	idx, term, isleader := rsm.rf.Start(op)
	if !isleader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	pendingOp := &PendingOp{id: op.Id, term: term, ch: make(chan any, 1)}
	rsm.addOp(idx, pendingOp)
	rsm.mu.Unlock()
	return rsm.waitOp(idx, pendingOp)
}

func (rsm *RSM) waitOp(idx int, pendingOp *PendingOp) (rpc.Err, any) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer rsm.removeOp(idx, pendingOp)

	for {
		select {
		case reply := <-pendingOp.ch:
			if reply == struct{}{} { // this op should be aborted
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, reply
		case <-ticker.C:
			term, isleader := rsm.rf.GetState()
			if !isleader || term != pendingOp.term {
				return rpc.ErrWrongLeader, nil
			}
		// when rf is killed, next submit will return quickly
		case <-rsm.shutdownCh:
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) addOp(idx int, pendingOp *PendingOp) {
	if oldPendingOp, ok := rsm.pendingOps[idx]; ok {
		oldPendingOp.notify(struct{}{})
	}
	rsm.pendingOps[idx] = pendingOp
}

func (rsm *RSM) removeOp(idx int, pendingOp *PendingOp) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if curPendingOp, ok := rsm.pendingOps[idx]; ok && curPendingOp == pendingOp {
		delete(rsm.pendingOps, idx)
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok || op.Req == nil {
				return
			}
			res := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			// if this rsm is not leader or not old leader(just a follower),
			// ok will be false and skip pendingOp notify here
			if pendingOp, ok := rsm.pendingOps[msg.CommandIndex]; ok {
				if pendingOp.id == op.Id {
					pendingOp.notify(res)
				} else {
					// old leader's pendingOp overwrite
					pendingOp.notify(struct{}{})
				}
			}

			// both leader and follower will do snapshot
			if rsm.maxraftstate >= 0 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				index := msg.CommandIndex
				snapshot := rsm.sm.Snapshot()
				// must launch a goroutine?
				go rsm.rf.Snapshot(index, snapshot)
			}
			rsm.mu.Unlock()
		} else { // snapShot apply
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Lock()
			for _, pendingOp := range rsm.pendingOps {
				pendingOp.notify(struct{}{})
			}
			clear(rsm.pendingOps)
			rsm.mu.Unlock()
		}
	}

	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, pendingOp := range rsm.pendingOps {
		pendingOp.notify(struct{}{})
	}
	clear(rsm.pendingOps)
	close(rsm.shutdownCh)
}
