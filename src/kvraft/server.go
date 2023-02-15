package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

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

type informCh struct {
	Err     string
	Cmd     string
	Value   string
	ClerkId int
	Seq     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int
	table       map[string]string
	Maxreq      map[int]int
	Inform      map[int]chan informCh
}

func (kv *KVServer) CheckInform(Index int, ClerkId int, Seq int) chan informCh {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Inform[Index]; !ok {
		DebugLog(dKVraft, "S%d KV make chan for C%d", kv.me, Index)
		kv.Inform[Index] = make(chan informCh, 1)
	}
	if _, ok := kv.Maxreq[ClerkId]; !ok {
		kv.Maxreq[ClerkId] = 0
	}
	return kv.Inform[Index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     "Get",
		Key:     args.Key,
		Value:   "",
	})
	reply.LeaderId = kv.rf.GetLeader()
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
			DebugLog(dKVraft, "S%d KV Get reply err %v", kv.me, reply.Err)
		}
	} else {
		reply.Err = "Not Leader"
		DebugLog(dKVraft, "S%d KV Not Leader", kv.me)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     args.Op,
		Key:     args.Key,
		Value:   args.Value,
	})
	reply.Index = index
	reply.LeaderId = kv.rf.GetLeader()
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
			DebugLog(dKVraft, "S%d KV PutAppend reply err %v", kv.me, reply.Err)
		}
	} else {
		reply.Err = "Not Leader"
		DebugLog(dKVraft, "S%d KV Not Leader", kv.me)
	}
}

func (kv *KVServer) GetState(args *GetStateArgs, reply *GetStateReply) {
	_, reply.IsLeader = kv.rf.GetState()
}

func (kv *KVServer) CheckClerkOpDuplicate(Clerk, Seq int) bool {
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

func (kv *KVServer) CheckSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.rf.RaftStateSize() >= kv.maxraftstate {
		return true
	} else {
		return false
	}
}

func (kv *KVServer) DecodeSnapshot(cmd *raft.ApplyMsg) {
	data := cmd.Snapshot
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var Maxreq map[int]int
	var table map[string]string
	e1 := d.Decode(&lastApplied)
	e2 := d.Decode(&Maxreq)
	e3 := d.Decode(&table)
	if e1 != nil || e2 != nil || e3 != nil {
		DebugLog(dError, "S%d T%d Read persistant fails, T:%v V:%v L:%v", kv.me, e1, e2, e3)
	} else {
		if kv.rf.CondInstallSnapshot(cmd.SnapshotIndex, cmd.SnapshotTerm, cmd.Snapshot) {
			kv.lastApplied = lastApplied
			kv.Maxreq = Maxreq
			kv.table = table
		}
	}
}

func (kv *KVServer) CreateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e1 := e.Encode(&kv.lastApplied)
	e2 := e.Encode(&kv.Maxreq)
	e3 := e.Encode(&kv.table)
	if e1 != nil || e2 != nil || e3 != nil {
		DebugLog(dError, "S%d KV get persist data fail, %v %v %v", kv.me, e1, e2, e3)
	}
	return w.Bytes()
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
		if cmd.CommandValid {
			kv.mu.Lock()
			op := cmd.Command.(Op)
			if cmd.CommandIndex <= kv.lastApplied {
				DebugLog(dKVraft, "S%d KV index %d duplicate", kv.me, cmd.CommandIndex)
				kv.mu.Unlock()
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
				DebugLog(dKVraft, "S%d KV apply %v, index %d", kv.me, op, cmd.CommandIndex)
			case "Put":
				if kv.CheckClerkOpDuplicate(op.ClerkId, op.Seq) {
					info.Err = "Duplicate"
				} else {
					kv.table[op.Key] = op.Value
					DebugLog(dKVraft, "S%d KV apply %v, index %d", kv.me, op.Cmd, cmd.CommandIndex)
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
					DebugLog(dKVraft, "S%d KV apply %v, index %d", kv.me, op.Cmd, cmd.CommandIndex)
				}
			default:
				log.Fatal("Error cmd")
			}
			_, isLeader := kv.rf.GetState()
			ch, ok := kv.Inform[cmd.CommandIndex]
			if ok && isLeader {
				DebugLog(dKVraft, "S%d KV notify index %d", kv.me, cmd.CommandIndex)
				ch <- info
			} else {
				DebugLog(dKVraft, "S%d KV has no ch in %d or isn't leader", kv.me, cmd.CommandIndex)
			}
			if kv.CheckSnapshot() {
				kv.rf.Snapshot(kv.lastApplied, kv.CreateSnapshot())
			}
			kv.mu.Unlock()
		}
		if cmd.SnapshotValid {
			kv.mu.Lock()
			kv.DecodeSnapshot(&cmd)
			kv.mu.Unlock()
		}
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
	kv.Inform = make(map[int]chan informCh)
	kv.Maxreq = make(map[int]int)
	kv.lastApplied = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	return kv
}
