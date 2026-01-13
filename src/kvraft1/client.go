package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

const RETRY_FREQ = 100 * time.Millisecond

// 4B passes without remembering the leader as well. If we were to remember it,
// we would need to add leaderId to struct and cycle through servers
// with id = (leaderID + i) % len(ck.servers) where i := range len(ck.servers)
type Clerk struct {
	clnt    *tester.Clnt
	servers []string

	me    uuid.UUID
	reqID int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.me = uuid.New()
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
	ck.reqID++
	args := rpc.GetArgs{
		Key:      key,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	DPrintf("-----------[Client-%v] starting Get(%s)\n", ck.me, key)
	for {
		for _, srvr := range ck.servers {
			reply := rpc.GetReply{}

			DPrintf("[Client-%v] Get(%s) to %s\n", ck.me, key, srvr)
			ok := ck.clnt.Call(srvr, "KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			DPrintf("[Client-%v] Get reply from %s: %v\n", ck.me, srvr, reply)

			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrWrongLeader:
				continue
			default:
				return reply.Value, reply.Version, reply.Err
			}
		}
		time.Sleep(RETRY_FREQ)
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
	ck.reqID++
	args := rpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		ClientID: ck.me,
		ReqID:    ck.reqID,
	}

	firstAttempt := true

	DPrintf("---------[Client-%v] starting Put(%s, %s)\n", ck.me, key, value)

	for {
		for _, srvr := range ck.servers {
			for {
				reply := rpc.PutReply{} // New object for each attempt
				DPrintf("[Client-%v] Put(%s, %s) to %s\n", ck.me, key, value, srvr)
				ok := ck.clnt.Call(srvr, "KVServer.Put", &args, &reply)
				if !ok {
					if !firstAttempt {
						break
					}
					// network may have discarded a successful response from the server, so try again
					firstAttempt = false
					continue
				}
				DPrintf("[Client-%v] Put reply from %s: %v\n", ck.me, srvr, reply)

				if reply.Err == rpc.ErrWrongLeader {
					break
				}
				if reply.Err == rpc.ErrVersion && !firstAttempt {
					return rpc.ErrMaybe
				}

				return reply.Err
			}
		}
		time.Sleep(RETRY_FREQ)
	}
}
