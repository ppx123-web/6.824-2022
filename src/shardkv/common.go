package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicate   = "ErrDuplicate"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int
	Seq     int
	Shard   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int
	Seq     int
	Shard   int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardTransferArg struct {
	ConfigN int
	Shard   int
}

type ShardTransferReply struct {
	Err    Err
	Succ   bool
	Table  map[string]string
	Maxreq map[int]int
	Shard  int
	CfgN   int
	GID    int
}

type DeleteArgs struct {
	CfgN  int
	Shard int
}

type DeleteReply struct {
	Succ bool
	Err  Err
}
