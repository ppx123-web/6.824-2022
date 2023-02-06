package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	me       int
	seq      int
	mu       sync.Mutex
}

var ClerkId = 0
var ClerkIdmu sync.Mutex

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = int(nrand()) % len(servers)
	ck.seq = 0

	ClerkIdmu.Lock()
	ck.me = ClerkId
	ClerkId += 1
	ClerkIdmu.Unlock()

	DebugLog(dInfo, "C%d Clerk init", ck.me)
	return ck
}

func (ck *Clerk) FindLeader(leaderId int) {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	DebugLog(dClient, "C%d set LeaderId %d", ck.me, ck.leaderId)
}

func (ck *Clerk) AllocateIndex() int {
	ck.mu.Lock()
	ck.seq++
	ret := ck.seq
	ck.mu.Unlock()
	return ret
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	var args = GetArgs{
		Key:     key,
		ClerkId: ck.me,
		Seq:     ck.AllocateIndex(),
	}
	for {
		var reply GetReply
		DebugLog(dClient, "C%d Get key:%v", ck.me, key)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case "Duplicate":
				return reply.Value
			case "":
				return reply.Value
			case "Not Match":
				DebugLog(dClient, "C%d Get Fail %v, retry", ck.me, reply.Err)
			case "Not Leader":
				ck.FindLeader(reply.LeaderId)
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args = PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.me,
		Seq:     ck.AllocateIndex(),
	}
	for {
		var reply PutAppendReply
		DebugLog(dClient, "C%d PutAppend %v %v to %d", ck.me, key, value, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case "Duplicate":
				return
			case "":
				return
			case "Not Match":
				DebugLog(dClient, "C%d Get Fail %v, retry", ck.me, reply.Err)
			case "Not Leader":
				ck.FindLeader(reply.LeaderId)
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
