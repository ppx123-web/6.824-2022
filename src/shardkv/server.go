package shardkv

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int
	Seq     int
	Cmd     string
	Key     string
	Value   string
}

type informCh struct {
	Err     string
	Cmd     string
	Value   string
	ClerkId int
	Seq     int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck         *shardctrler.Clerk
	dead        int32
	lastApplied int
	table       map[string]string
	Maxreq      map[int]int
	Inform      map[int]chan informCh
}

func (kv *ShardKV) CheckShardsGid(key string) bool {
	shard := key2shard(key)
	config := kv.mck.Query(-1)
	gid := config.Shards[shard]
	return kv.gid == gid
}

func (kv *ShardKV) CheckInform(Index int, ClerkId int, Seq int) chan informCh {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Inform[Index]; !ok {
		kv.Inform[Index] = make(chan informCh, 1)
	}
	if _, ok := kv.Maxreq[ClerkId]; !ok {
		kv.Maxreq[ClerkId] = 0
	}
	return kv.Inform[Index]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.CheckShardsGid(args.Key) {
		reply.Err = ErrWrongGroup
	}
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     "Get",
		Key:     args.Key,
		Value:   "",
	})
	if isLeader {
		ch := kv.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			kv.mu.Lock()
			close(ch)
			delete(kv.Inform, index)
			kv.mu.Unlock()
			reply.Value = info.Value
		case <-time.After(50 * time.Millisecond):
			reply.Err = "Timeout"
		}
	} else {
		reply.Err = "ErrWrongLeader"
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.CheckShardsGid(args.Key) {
		reply.Err = ErrWrongGroup
	}
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     args.Op,
		Key:     args.Key,
		Value:   args.Value,
	})
	if isLeader {
		ch := kv.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			kv.mu.Lock()
			close(ch)
			delete(kv.Inform, index)
			kv.mu.Unlock()
		case <-time.After(50 * time.Millisecond):
			reply.Err = "Timeout"
		}
	} else {
		reply.Err = "ErrWrongLeader"
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) CheckClerkOpDuplicate(Clerk, Seq int) bool {
	if _, ok := kv.Maxreq[Clerk]; !ok {
		kv.Maxreq[Clerk] = 0
	}
	if kv.Maxreq[Clerk] >= Seq {
		return true
	} else {
		kv.Maxreq[Clerk] = Seq
		return false
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		cmd := <-kv.applyCh
		if cmd.CommandValid {
			kv.mu.Lock()
			op := cmd.Command.(Op)
			if cmd.CommandIndex <= kv.lastApplied {
				continue
			}
			kv.lastApplied = cmd.CommandIndex
			var info = informCh{
				Cmd:     op.Cmd,
				ClerkId: op.ClerkId,
				Seq:     op.Seq,
				Err:     "",
			}
			switch op.Cmd {
			case "Get":
				if v, ok := kv.table[op.Key]; ok {
					info.Value = v
				} else {
					info.Value = ""
				}
			case "Put":
				if kv.CheckClerkOpDuplicate(op.ClerkId, op.Seq) {
					info.Err = "Duplicate"
				} else {
					kv.table[op.Key] = op.Value
				}
			case "Append":
				if kv.CheckClerkOpDuplicate(op.ClerkId, op.Seq) {
					info.Err = "Duplicate"
				} else {
					if v, ok := kv.table[op.Key]; ok {
						kv.table[op.Key] = v + op.Value
					} else {
						kv.table[op.Key] = op.Value
					}
				}
			default:
				log.Fatal("Error cmd")
			}
			_, isLeader := kv.rf.GetState()
			ch, ok := kv.Inform[cmd.CommandIndex]
			if ok && isLeader {
				ch <- info
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.table = make(map[string]string)
	kv.Inform = make(map[int]chan informCh)
	kv.Maxreq = make(map[int]int)
	kv.lastApplied = 0

	go kv.applier()

	return kv
}
