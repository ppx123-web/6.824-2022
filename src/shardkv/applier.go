package shardkv

import (
	"log"

	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) CheckClerkOpDuplicate(Clerk, Seq int) bool {
	if _, ok := kv.Maxreq[Clerk]; !ok {
		kv.Maxreq[Clerk] = 0
	}
	if kv.Maxreq[Clerk] >= Seq {
		DebugLog(dKVraft, "G%d S%d KV Clerk %d Seq %d duplicate", kv.gid, kv.me, Clerk, Seq)
		return true
	} else {
		kv.Maxreq[Clerk] = Seq
		return false
	}
}

func (kv *ShardKV) DataOperation(cmd raft.ApplyMsg) {
	op := cmd.Command.(Op)
	var info = informCh{
		Cmd:     op.Cmd,
		ClerkId: op.ClerkId,
		Seq:     op.Seq,
		Err:     OK,
	}
	shard := key2shard(op.Key)
	if kv.Respon[shard] {
		switch op.Cmd {
		case "Get":
			if v, ok := kv.table[op.Key]; ok {
				info.Value = v
			} else {
				info.Value = ""
				info.Err = ErrNoKey
			}
			DebugLog(dKVraft, "G%d S%d KV apply %v, shard %d, index %d", kv.gid, kv.me, op, shard, cmd.CommandIndex)
		case "Put":
			if kv.CheckClerkOpDuplicate(op.ClerkId, op.Seq) {
				info.Err = ErrDuplicate
			} else {
				kv.table[op.Key] = op.Value
				DebugLog(dKVraft, "G%d S%d KV apply %v, shard %d, index %d", kv.gid, kv.me, op, shard, cmd.CommandIndex)
			}
		case "Append":
			if kv.CheckClerkOpDuplicate(op.ClerkId, op.Seq) {
				info.Err = ErrDuplicate
			} else {
				if v, ok := kv.table[op.Key]; ok {
					kv.table[op.Key] = v + op.Value
				} else {
					kv.table[op.Key] = op.Value
				}
				DebugLog(dKVraft, "G%d S%d KV apply %v, shard %d, index %d, get %v", kv.gid, kv.me, op, shard, cmd.CommandIndex, kv.table[op.Key])
			}
		default:
			log.Fatal("Error cmd")
		}
	} else {
		info.Err = ErrWrongGroup
		DebugLog(dKVraft, "G%d S%d KV WrongGroup, %v shard %d, acc %v", kv.gid, kv.me, op.Key, key2shard(op.Key), kv.Respon)
	}

	_, isLeader := kv.rf.GetState()
	ch, ok := kv.Inform[cmd.CommandIndex]
	if ok && isLeader {
		DebugLog(dKVraft, "G%d S%d KV notify index %d", kv.gid, kv.me, cmd.CommandIndex)
		ch <- info
	}
	// } else {
	// 	DebugLog(dKVraft, "G%d S%d KV has no ch in %d or isn't leader", kv.gid , cmd.CommandIndex)
	// }
}

func (kv *ShardKV) UpdateConfig(cfg shardctrler.Config) {
	prevcfg := kv.cfg
	if cfg.Num <= prevcfg.Num {
		return
	}
	kv.cfg = cfg
	outShards := []int{} //需要存储等待其他gourp请求的数据
	for shard, gid := range cfg.Shards {
		if gid == kv.gid {
			//当前cfg中，属于gid的shards
			if prevcfg.Num != 0 && !kv.Respon[shard] {
				kv.InShard[shard] = prevcfg.Num
			} else if prevcfg.Num == 0 {
				kv.Respon[shard] = true
			}
		} else if gid != kv.gid && kv.Respon[shard] {
			//当前cfg中，不属于gid但之前属于gid的shards
			outShards = append(outShards, shard)
			kv.Respon[shard] = false
		}
	}
	DebugLog(dKVraft, "G%d S%d Update Config to %d, need %v, out %v", kv.gid, kv.me, kv.cfg.Num, kv.InShard, outShards)
	if len(outShards) > 0 {
		kv.ShardDB[prevcfg.Num] = make(map[int]map[string]string)
		for _, shard := range outShards {
			t := make(map[string]string)
			for k, v := range kv.table {
				if key2shard(k) == shard {
					t[k] = v
					delete(kv.table, k)
				}
			}
			kv.ShardDB[prevcfg.Num][shard] = t
			DebugLog(dKVraft, "G%d S%d Save table Config %d, %v", kv.gid, kv.me, prevcfg.Num, t)
		}
	}
}

func (kv *ShardKV) UpdateShards(shard, cfgN int, table map[string]string, maxreq map[int]int) {
	if cfgN != kv.cfg.Num-1 || kv.Respon[shard] {
		return
	}
	delete(kv.InShard, shard)
	for k, v := range table {
		kv.table[k] = v
	}
	for k, v := range maxreq {
		if v > kv.Maxreq[k] {
			kv.Maxreq[k] = v
		}
	}
	kv.Respon[shard] = true
	DebugLog(dKVraft, "G%d S%d update cfg %d shards %d to %v", kv.gid, kv.me, cfgN, shard, table)
	kv.NoNeed[DeleteArgs{CfgN: cfgN, Shard: shard}] = true
}

func (kv *ShardKV) DeleteDB(cfgN, shard, index int) {
	delete(kv.ShardDB[cfgN], shard)
	if len(kv.ShardDB[cfgN]) == 0 {
		delete(kv.ShardDB, cfgN)
	}
	_, isLeader := kv.rf.GetState()
	ch, ok := kv.Inform[index]
	if ok && isLeader {
		DebugLog(dKVraft, "G%d S%d KV notify index %d", kv.gid, kv.me, index)
		ch <- informCh{
			Err: OK,
		}
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		cmd := <-kv.applyCh
		if cmd.CommandValid {
			kv.mu.Lock()
			if cmd.CommandIndex <= kv.lastApplied {
				DebugLog(dKVraft, "G%d S%d KV index %d duplicate", kv.gid, kv.me, cmd.CommandIndex)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = cmd.CommandIndex
			if _, ok := cmd.Command.(Op); ok {
				kv.DataOperation(cmd)
			} else if cfg, ok := cmd.Command.(shardctrler.Config); ok {
				kv.UpdateConfig(cfg)
			} else if reply, ok := cmd.Command.(ShardTransferReply); ok {
				kv.UpdateShards(reply.Shard, reply.CfgN, reply.Table, reply.Maxreq)
			} else if reply, ok := cmd.Command.(DeleteArgs); ok {
				kv.DeleteDB(reply.CfgN, reply.Shard, cmd.CommandIndex)
			}
			if kv.CheckSnapshot() {
				kv.rf.Snapshot(kv.lastApplied, kv.CreateSnapshot())
			}
			kv.mu.Unlock()
		} else if cmd.SnapshotValid {
			kv.mu.Lock()
			kv.DecodeSnapshot(&cmd)
			kv.mu.Unlock()
		}
	}
}
