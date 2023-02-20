package shardkv

import (
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
	Err     Err
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
	cfg         shardctrler.Config
	ShardDB     map[int]map[int]map[string]string // config -> shard -> table
	Respon      [shardctrler.NShards]bool         // offer service for shard
	InShard     map[int]int                       // shard -> cfg.Num
	NoNeed      map[DeleteArgs]bool
}

func (kv *ShardKV) CheckShardsGid(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(key)
	gid := kv.cfg.Shards[shard]
	if kv.gid == gid && kv.Respon[shard] {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) CheckInform(Index int, ClerkId int, Seq int) chan informCh {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Inform[Index]; !ok {
		DebugLog(dKVraft, "G%d S%d KV make chan for C%d", kv.gid, kv.me, Index)
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
		DebugLog(dKVraft, "G%d S%d KV Get %v, shard %d, index %d", kv.gid, kv.me, args.Key, args.Shard, index)
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
			reply.Err = ErrTimeOut
			DebugLog(dKVraft, "G%d S%d KV Get reply err %v", kv.gid, kv.me, reply.Err)
		}
	} else {
		reply.Err = ErrWrongLeader
		DebugLog(dKVraft, "G%d S%d KV Not Leader", kv.gid, kv.me, kv.me)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.CheckShardsGid(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     args.Op,
		Key:     args.Key,
		Value:   args.Value,
	})
	if isLeader {
		DebugLog(dKVraft, "G%d S%d KV Get %v, shard %d, index %d", kv.gid, kv.me, args.Key, args.Shard, index)
		ch := kv.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			kv.mu.Lock()
			close(ch)
			delete(kv.Inform, index)
			kv.mu.Unlock()
		case <-time.After(50 * time.Millisecond):
			reply.Err = ErrTimeOut
			DebugLog(dKVraft, "G%d S%d KV Get reply err %v", kv.gid, kv.me, reply.Err)
		}
	} else {
		reply.Err = ErrWrongLeader
		DebugLog(dKVraft, "G%d S%d KV Not Leader", kv.gid, kv.me)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// atomic.StoreInt32(&kv.dead, 1)
	DebugLog(dInfo, "G%d S%d killed", kv.gid, kv.me)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func copyStr2Str(m map[string]string) map[string]string {
	ret := make(map[string]string)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

func copyInt2Int(m map[int]int) map[int]int {
	ret := make(map[int]int)
	for k, v := range m {
		ret[k] = v
	}
	return ret
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardTransferReply{})
	labgob.Register(DeleteArgs{})

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

	kv.cfg = shardctrler.Config{}
	kv.ShardDB = make(map[int]map[int]map[string]string)
	kv.InShard = make(map[int]int)
	kv.NoNeed = make(map[DeleteArgs]bool)

	DebugLog(dInfo, "G%d S%d start", kv.gid, kv.me)

	go kv.WatchConfig()
	go kv.applier()
	go kv.pullShards()
	go kv.garbageCollection()
	go kv.EmptyAppend()

	return kv
}
