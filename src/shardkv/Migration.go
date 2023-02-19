package shardkv

import (
	"sync"
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) WatchConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		config := kv.mck.Query(kv.cfg.Num + 1)
		if _, isLeader := kv.rf.GetState(); isLeader && len(kv.InShard) == 0 && kv.cfg.Num < config.Num {
			DebugLog(dKVraft, "G%d S%d start config %d", kv.gid, kv.me, config.Num)
			kv.rf.Start(config)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) SendShardMigration(args *ShardTransferArg, config *shardctrler.Config) ShardTransferReply {
	for {
		gid := config.Shards[args.Shard]
		if servers, ok := config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ShardTransferReply
				ok := srv.Call("ShardKV.ShardMigration", args, &reply)
				if ok && reply.Succ && reply.Err == OK {
					return reply
				}
			}
		}
	}
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
	DebugLog(dKVraft, "G%d S%d KV reply table maxreq", kv.gid, kv.me)
}

func (kv *ShardKV) pullShards() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && len(kv.InShard) > 0 {
			var wait sync.WaitGroup
			for shard, cfgN := range kv.InShard {
				DebugLog(dKVraft, "G%d S%d KV start pull shards %d, cfg %d", kv.gid, kv.me, shard, cfgN)
				wait.Add(1)
				go func(shard int, cfg shardctrler.Config) {
					defer wait.Done()
					reply := kv.SendShardMigration(&ShardTransferArg{
						ConfigN: cfg.Num,
						Shard:   shard,
					}, &cfg)
					kv.rf.Start(reply)
					DebugLog(dKVraft, "G%d S%d KV pull shards succ %d, cfg %d, table %v", kv.gid, kv.me, reply.Shard, reply.CfgN, reply.Table)
				}(shard, kv.mck.Query(cfgN))
			}
			kv.mu.Unlock()
			wait.Wait()
			DebugLog(dKVraft, "G%d S%d KV pull all succ", kv.gid, kv.me)
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
					if servers, ok := kv.cfg.Groups[kv.mck.Query(args.CfgN).Shards[args.Shard]]; ok {
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

			// for cfgN, m := range kv.ShardDB {
			// 	for shard := range m {
			// 		wait.Add(1)
			// 		go func(args DeleteArgs) {
			// 			defer wait.Done()
			// 			if servers, ok := kv.cfg.Groups[kv.mck.Query(args.CfgN).Shards[args.Shard]]; ok {
			// 				for si := 0; si < len(servers); si++ {
			// 					srv := kv.make_end(servers[si])
			// 					var reply ShardTransferReply
			// 					ok := srv.Call("ShardKV.CanDelete", &args, &reply)
			// 					if ok && reply.Succ && reply.Err == OK {
			// 						kv.rf.Start(args)
			// 					}
			// 				}
			// 			}
			// 		}(DeleteArgs{
			// 			CfgN:  cfgN,
			// 			Shard: shard,
			// 		})
			// 	}
			// }
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
