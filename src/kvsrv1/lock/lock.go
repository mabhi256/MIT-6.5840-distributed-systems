package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	clientID string
	Key      string
	Version  rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		clientID: kvtest.RandValue(8),
		Key:      l,
	}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		locker, version, err := lk.ck.Get(lk.Key)
		// Will get ErrNoKey if nobody has requested a lock yet
		if err != rpc.OK && err != rpc.ErrNoKey {
			continue
		}

		// If we already hold the lock, then update version
		// There may be a mismatch if the the lock succeeded but we didn't get a response
		if locker == lk.clientID {
			lk.Version = version
			break
		}

		// If this is the Clerk's 1st lock ever, or no one has lock
		if err == rpc.ErrNoKey || locker == "" {
			err2 := lk.ck.Put(lk.Key, lk.clientID, version)
			if err2 == rpc.OK {
				lk.Version = version + 1
				time.Sleep(10 * time.Millisecond) // Allow operation to be recorded
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		locker, version, err := lk.ck.Get(lk.Key)
		if err != rpc.OK {
			continue
		}

		if locker == lk.clientID && version == lk.Version {
			err2 := lk.ck.Put(lk.Key, "", lk.Version)
			if err2 == rpc.OK {
				lk.Version = 0

				// Allow operation to be recorded by sleeping. If we don't sleep, then it fails the test.
				// Another option is to not sleep and not return here, and let the loop continue.
				// In the next Get it will find that the lock is "" and exit the loop.
				// But this adds another unreliable RTT
				time.Sleep(10 * time.Millisecond)
				return
			}
		} else {
			// We don't have the lock
			return
		}
	}
}
