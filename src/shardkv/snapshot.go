package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

func (kv *ShardKV) CheckSnapshot() bool {
	if kv.maxraftstate == 1 {
		return true
	}
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.rf.RaftStateSize() >= kv.maxraftstate {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) DecodeSnapshot(cmd *raft.ApplyMsg) {
	data := cmd.Snapshot
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var Maxreq map[int]int
	var table map[string]string

	var ShardDB map[int]map[int]map[string]string
	var cfg shardctrler.Config
	var Respon [shardctrler.NShards]bool
	var InShard map[int]int
	var NoNeed map[DeleteArgs]bool
	e1 := d.Decode(&lastApplied)
	e2 := d.Decode(&Maxreq)
	e3 := d.Decode(&table)

	e4 := d.Decode(&ShardDB)
	e5 := d.Decode(&cfg)
	e6 := d.Decode(&Respon)
	e7 := d.Decode(&InShard)
	e8 := d.Decode(&NoNeed)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil || e8 != nil {
		DebugLog(dError, "G%d S%d Read persistant fails, %v %v %v %v %v %v %v %v", kv.gid, kv.me, e1, e2, e3, e4, e5, e6, e7)
	} else {
		if kv.rf.CondInstallSnapshot(cmd.SnapshotIndex, cmd.SnapshotTerm, cmd.Snapshot) {
			kv.lastApplied = lastApplied
			kv.Maxreq = Maxreq
			kv.table = table
			kv.ShardDB = ShardDB
			kv.cfg = cfg
			copy(kv.Respon[:], Respon[:])
			kv.InShard = InShard
			kv.NoNeed = NoNeed
			DebugLog(dSnap, "G%d S%d install snapshot, cfg %d, respon %v, Inshard %v, index %d", kv.gid, kv.me, kv.cfg.Num, kv.Respon, kv.InShard, kv.lastApplied)

		}
	}
}

func (kv *ShardKV) CreateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e1 := e.Encode(&kv.lastApplied)
	e2 := e.Encode(&kv.Maxreq)
	e3 := e.Encode(&kv.table)

	e4 := e.Encode(&kv.ShardDB)
	e5 := e.Encode(&kv.cfg)
	e6 := e.Encode(&kv.Respon)
	e7 := e.Encode(&kv.InShard)
	e8 := e.Encode(&kv.NoNeed)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil || e8 != nil {
		DebugLog(dError, "G%d S%d get persist data fail, %v %v %v %v %v %v %v %v", kv.gid, kv.me, e1, e2, e3, e4, e5, e6, e7)
	}
	DebugLog(dSnap, "G%d S%d create snapshot, cfg %d, respon %v, Inshard %v, index %d", kv.gid, kv.me, kv.cfg.Num, kv.Respon, kv.InShard, kv.lastApplied)
	return w.Bytes()
}
