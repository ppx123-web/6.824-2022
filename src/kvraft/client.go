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

func (ck *Clerk) FindLeader() {
	for index, server := range ck.servers {
		var reply GetStateReply
		ok := server.Call("KVServer.GetState", &GetStateArgs{}, &reply)
		if ok && reply.IsLeader {
			ck.leaderId = index
			return
		}
	}
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
		Key: key,
	}
	for {
		var reply GetReply
		DebugLog(dClient, "C%d Get key:%v to KV%d", ck.me, key, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && reply.IsLeader {
			return reply.Value
		}
		if ok && !reply.IsLeader {
			ck.FindLeader()
			DebugLog(dClient, "C%d Leader ID %d", ck.me, ck.leaderId)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ck.leaderId = int(nrand()) % len(ck.servers)
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
	}
	for {
		var reply PutAppendReply
		DebugLog(dClient, "C%d PutAppend %v %v to KV %d", ck.me, key, value, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.IsLeader {
			break
		}
		if ok && !reply.IsLeader {
			ck.FindLeader()
			DebugLog(dClient, "C%d Leader ID %d", ck.me, ck.leaderId)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ck.leaderId = int(nrand()) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
