package shardkv

import (
	"sync"
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) WatchConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		next := kv.cfg.Num + 1
		kv.mu.Unlock()
		config := kv.mck.Query(next)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && len(kv.InShard) == 0 && kv.cfg.Num < config.Num {
			DebugLog(dKVraft, "G%d S%d start config %d", kv.gid, kv.me, config.Num)
			kv.rf.Start(config)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) SendShardMigration(args *ShardTransferArg, config *shardctrler.Config) (bool, ShardTransferReply) {
	gid := config.Shards[args.Shard]
	if servers, ok := config.Groups[gid]; ok {
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ShardTransferReply
			DebugLog(dKVraft, "G%d S%d try pull shards %d, cfg %d", kv.gid, kv.me, args.Shard, args.ConfigN)
			ok := srv.Call("ShardKV.ShardMigration", args, &reply)
			if ok && reply.Succ && reply.Err == OK {
				return true, reply
			}
		}
	}
	return false, ShardTransferReply{}
}

func (kv *ShardKV) ShardMigration(args *ShardTransferArg, reply *ShardTransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Succ = false
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigN > kv.cfg.Num-1 {
		reply.Succ = false
		return
	}
	reply.Table = copyStr2Str(kv.ShardDB[args.ConfigN][args.Shard])
	reply.Maxreq = copyInt2Int(kv.Maxreq)
	reply.Succ = true
	reply.Err = OK
	reply.Shard = args.Shard
	reply.CfgN = args.ConfigN
	DebugLog(dKVraft, "G%d S%d reply cfg %d, shard %d", kv.gid, kv.me, args.ConfigN, args.Shard)
}

func (kv *ShardKV) pullShards() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && len(kv.InShard) > 0 {
			var wait sync.WaitGroup
			for shard, cfgN := range kv.InShard {
				DebugLog(dKVraft, "G%d S%d start pull shards %d, cfg %d", kv.gid, kv.me, shard, cfgN)
				wait.Add(1)
				go func(shard int, cfg shardctrler.Config) {
					defer wait.Done()
					ok, reply := kv.SendShardMigration(&ShardTransferArg{
						ConfigN: cfg.Num,
						Shard:   shard,
					}, &cfg)
					if ok {
						index, _, _ := kv.rf.Start(reply)
						DebugLog(dKVraft, "G%d S%d pull shards succ %d, cfg %d, table %v, index %d", kv.gid, kv.me, reply.Shard, reply.CfgN, reply.Table, index)
					}
				}(shard, kv.mck.Query(cfgN))
			}
			kv.mu.Unlock()
			wait.Wait()
			DebugLog(dKVraft, "G%d S%d pull all succ", kv.gid, kv.me)
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) garbageCollection() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var wait sync.WaitGroup
			for args := range kv.NoNeed {
				wait.Add(1)
				go func(args DeleteArgs) {
					defer wait.Done()
					cfg := kv.mck.Query(args.CfgN)
					if servers, ok := cfg.Groups[cfg.Shards[args.Shard]]; ok {
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							var reply ShardTransferReply
							ok := srv.Call("ShardKV.CanDelete", &args, &reply)
							if ok && reply.Succ && reply.Err == OK {
								kv.mu.Lock()
								delete(kv.NoNeed, args)
								kv.mu.Unlock()
							}
						}
					}
				}(args)
			}
			kv.mu.Unlock()
			wait.Wait()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) CanDelete(args *DeleteArgs, reply *DeleteReply) {
	index, _, isLeader := kv.rf.Start(*args)
	reply.Err = ErrWrongLeader
	reply.Succ = false
	if isLeader {
		ch := kv.CheckInform(index, -1, -1)
		info := <-ch
		reply.Err = Err(info.Err)
		kv.mu.Lock()
		close(ch)
		delete(kv.Inform, index)
		kv.mu.Unlock()
		reply.Err = OK
		reply.Succ = true
	}

}

func (kv *ShardKV) EmptyAppend() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader && !kv.rf.HasCurrentTermLog() {
			kv.rf.Start(Op{
				Cmd: "Empty",
			})
		}
		time.Sleep(100 * time.Millisecond)
	}
}
