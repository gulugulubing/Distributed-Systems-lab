package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockId string // 多个client，可能都会去处理同一个订单，那就会重复。这里就用这个，标识竞争的资源
	// 一个lockId,在一个时间只能对应一个clientId
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockId = l
	lk.clientId = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {

	// Your code here
	for {
		curClientId, version, getErr := lk.ck.Get(lk.lockId)
		switch getErr {
		case rpc.ErrNoKey: // lock is free, no one has ever acquire it
			// other client interleaving may occur here, if it happens, try again
			if putErr := lk.ck.Put(lk.lockId, lk.clientId, 0); putErr == rpc.OK {
				return
			}
			// purErr may be ErrVersion，ErrMaybe here, no possible for ErrNoKey, because version is 0
			// ErrMaybe may be due to interleaving by others, maybe has acquired not lost reply and put again
			// if it is acquired, never mind, loop next round, this client will know it has acquired the lock
		case rpc.OK:
			// other client interleaving may occur here, if it happens, try again
			if version%2 == 0 { // even version means, this lock is free
				// other client interleaving may occur here, if it happens, try again
				if putErr := lk.ck.Put(lk.lockId, lk.clientId, version); putErr == rpc.OK {
					return
				}
			}

			if version%2 != 0 && curClientId == lk.clientId { // not free, but already acquired by this client
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	curClientId, version, getErr := lk.ck.Get(lk.lockId)
	if getErr == rpc.ErrNoKey {
		// nothing to release
		return
	}
	if getErr == rpc.OK {
		if curClientId == lk.clientId {
			// lockId match, could release
			lk.ck.Put(lk.lockId, lk.clientId, version)
			// this put may return OK or ErrMaybe, but ErrMaybe must mean put has already done, the Err just due to retry
		} else {
			// not match, should not release
			return
		}
	}
}
