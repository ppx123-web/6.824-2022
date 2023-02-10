package shardctrler

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead        int32
	lastApplied int
	table       map[string]string
	Maxreq      map[int]int
	Inform      map[int]chan informCh

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) CheckInform(Index int, ClerkId int, Seq int) chan informCh {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.Inform[Index]; !ok {
		DebugLog(dKVraft, "S%d KV make chan for C%d", sc.me, Index)
		sc.Inform[Index] = make(chan informCh, 1)
	}
	if _, ok := sc.Maxreq[ClerkId]; !ok {
		sc.Maxreq[ClerkId] = 0
	}
	return sc.Inform[Index]
}

type informCh struct {
	Err     string
	Cmd     Command
	ClerkId int
	Seq     int
	reply   Config
}

type Command int

const (
	Join Command = iota + 1
	Leave
	Move
	Query
)

type Op struct {
	// Your data here.
	ClerkId int
	Seq     int
	Cmd     Command
	Servers map[int][]string
	GIDs    []int
	GID     int
	Shard   int
	Num     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DebugLog(dKVraft, "S%d KV Join %d, ClerkId %d,Seq %d", sc.me, args.Servers, args.ClerkId, args.Seq)
	index, _, isLeader := sc.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     Join,
		Servers: args.Servers,
	})
	if isLeader {
		ch := sc.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			sc.mu.Lock()
			close(ch)
			delete(sc.Inform, index)
			sc.mu.Unlock()
		case <-time.After(200 * time.Millisecond):
			reply.Err = "Timeout"
			DebugLog(dKVraft, "S%d KV Join reply err %v", sc.me, reply.Err)
		}
	} else {
		reply.Err = "Not Leader"
		reply.WrongLeader = true
		DebugLog(dKVraft, "S%d KV Not Leader", sc.me)
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DebugLog(dKVraft, "S%d KV Leave %d, ClerkId %d,Seq %d", sc.me, args.GIDs, args.ClerkId, args.Seq)
	index, _, isLeader := sc.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     Leave,
		GIDs:    args.GIDs,
	})
	if isLeader {
		ch := sc.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			sc.mu.Lock()
			close(ch)
			delete(sc.Inform, index)
			sc.mu.Unlock()
		case <-time.After(200 * time.Millisecond):
			reply.Err = "Timeout"
			DebugLog(dKVraft, "S%d KV Leave reply err %v", sc.me, reply.Err)
		}
	} else {
		reply.Err = "Not Leader"
		reply.WrongLeader = true
		DebugLog(dKVraft, "S%d KV Not Leader", sc.me)
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DebugLog(dKVraft, "S%d KV Move S%d -> G%d, ClerkId %d,Seq %d", sc.me, args.Shard, args.GID, args.ClerkId, args.Seq)
	index, _, isLeader := sc.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     Move,
		Shard:   args.Shard,
		GID:     args.GID,
	})
	if isLeader {
		ch := sc.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			sc.mu.Lock()
			close(ch)
			delete(sc.Inform, index)
			sc.mu.Unlock()
		case <-time.After(200 * time.Millisecond):
			reply.Err = "Timeout"
			DebugLog(dKVraft, "S%d KV Move reply err %v", sc.me, reply.Err)
		}
	} else {
		reply.Err = "Not Leader"
		reply.WrongLeader = true
		DebugLog(dKVraft, "S%d KV Not Leader", sc.me)
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DebugLog(dKVraft, "S%d KV Query %d, ClerkId %d,Seq %d", sc.me, args.Num, args.ClerkId, args.Seq)
	index, _, isLeader := sc.rf.Start(Op{
		ClerkId: args.ClerkId,
		Seq:     args.Seq,
		Cmd:     Query,
		Num:     args.Num,
	})
	if isLeader {
		ch := sc.CheckInform(index, args.ClerkId, args.Seq)
		select {
		case info := <-ch:
			reply.Err = Err(info.Err)
			reply.Config = info.reply
			sc.mu.Lock()
			close(ch)
			delete(sc.Inform, index)
			sc.mu.Unlock()
		case <-time.After(200 * time.Millisecond):
			reply.Err = "Timeout"
			DebugLog(dKVraft, "S%d KV Query reply err %v", sc.me, reply.Err)
		}
	} else {
		reply.Err = "Not Leader"
		reply.WrongLeader = true
		DebugLog(dKVraft, "S%d KV Not Leader", sc.me)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		cmd := <-sc.applyCh
		if cmd.CommandValid {
			sc.mu.Lock()
			op := cmd.Command.(Op)
			if cmd.CommandIndex <= sc.lastApplied {
				DebugLog(dKVraft, "S%d KV index %d duplicate", sc.me, cmd.CommandIndex)
				continue
			}
			sc.lastApplied = cmd.CommandIndex
			var info = informCh{
				Cmd:     op.Cmd,
				ClerkId: op.ClerkId,
				Seq:     op.Seq,
				Err:     "",
			}
			switch op.Cmd {
			case Join:
				if !sc.CheckClerkOpDuplicate(info.ClerkId, info.Seq) {
					DebugLog(dKVraft, "S%d KV Join %v", sc.me, op.Servers)
					sc.JoinOperation(op.Servers)
				}
			case Leave:
				if !sc.CheckClerkOpDuplicate(info.ClerkId, info.Seq) {
					DebugLog(dKVraft, "S%d KV Leave %v", sc.me, op.GIDs)
					sc.LeaveOperation(op.GIDs)
				}
			case Move:
				if !sc.CheckClerkOpDuplicate(info.ClerkId, info.Seq) {
					DebugLog(dKVraft, "S%d KV Move %v %v", sc.me, op.Shard, op.GID)
					sc.MoveOperation(op.Shard, op.GID)
				}
			case Query:
				info.reply = sc.Queryperation(op.Num)
				DebugLog(dKVraft, "S%d KV Query %v, %v, Shards %v", sc.me, op.Num, info.reply.Groups, info.reply.Shards)
			default:
				log.Fatal("Error cmd")
			}
			_, isLeader := sc.rf.GetState()
			ch, ok := sc.Inform[cmd.CommandIndex]
			if ok && isLeader {
				DebugLog(dKVraft, "S%d KV notify index %d", sc.me, cmd.CommandIndex)
				ch <- info
			} else {
				DebugLog(dKVraft, "S%d KV has no ch in %d or isn't leader", sc.me, cmd.CommandIndex)
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) CheckClerkOpDuplicate(Clerk, Seq int) bool {
	if _, ok := sc.Maxreq[Clerk]; !ok {
		sc.Maxreq[Clerk] = 0
	}
	if sc.Maxreq[Clerk] >= Seq {
		return true
	} else {
		sc.Maxreq[Clerk] = Seq
		return false
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.table = make(map[string]string)
	sc.Inform = make(map[int]chan informCh)
	sc.Maxreq = make(map[int]int)
	sc.lastApplied = 0

	go sc.applier()

	return sc
}
