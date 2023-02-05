package kvraft

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table  map[string]string
	Inform map[int]chan Op
}

func (kv *KVServer) CheckInform(Index int) {
	if _, ok := kv.Inform[Index]; !ok {
		DebugLog(dKVraft, "S%d KV make chan for C%d", kv.me, Index)
		kv.Inform[Index] = make(chan Op)
	}
}

func (kv *KVServer) GetInform(index int) *Op {
	op := <-kv.Inform[index]
	return &op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DebugLog(dKVraft, "S%d KV Get %v", kv.me, args.Key)
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     "Get",
		Key:     args.Key,
		Value:   "",
	})
	kv.CheckInform(index)
	reply.IsLeader = isLeader
	reply.LeaderId = kv.rf.GetLeader()
	if reply.IsLeader {
		op := kv.GetInform(index)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if value, ok := kv.table[op.Key]; ok {
			reply.Value = value
		} else {
			reply.Value = ""
		}
	} else {
		DebugLog(dKVraft, "KV %d Not Leader, Leader is %d", kv.me, reply.LeaderId)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DebugLog(dKVraft, "S%d KV PutAppend", kv.me)
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     args.Op,
		Key:     args.Key,
		Value:   args.Value,
	})
	kv.CheckInform(index)
	reply.Index = index
	reply.IsLeader = isLeader
	reply.LeaderId = kv.rf.GetLeader()
	if reply.IsLeader {
		kv.Handler(kv.GetInform(index))
	} else {
		DebugLog(dKVraft, "KV %d Not Leader, Leader is %d", kv.me, reply.LeaderId)
	}
}

func (kv *KVServer) Handler(op *Op) {
	if op.Cmd == "Put" {
		DebugLog(dKVraft, "KV %d Put", kv.me)
		kv.mu.Lock()
		kv.table[op.Key] = op.Value
		kv.mu.Unlock()
	} else if op.Cmd == "Append" {
		DebugLog(dKVraft, "KV %d Append", kv.me)
		kv.mu.Lock()
		kv.table[op.Key] = kv.table[op.Key] + op.Value
		kv.mu.Unlock()
	}
}

func (kv *KVServer) GetState(args *GetStateArgs, reply *GetStateReply) {
	_, reply.IsLeader = kv.rf.GetState()
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
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		cmd := <-kv.applyCh
		op := cmd.Command.(Op)
		DebugLog(dKVraft, "S%d KV inform index %d", kv.me, cmd.CommandIndex)
		kv.Inform[cmd.CommandIndex] <- op
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.table = make(map[string]string)
	kv.Inform = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	return kv
}
