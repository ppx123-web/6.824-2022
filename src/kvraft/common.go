package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int
	Seq     int
}

type PutAppendReply struct {
	Err      Err
	Index    int
	IsLeader bool
	LeaderId int
}

type GetArgs struct {
	Key     string
	ClerkId int
	Seq     int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
	IsLeader bool
	LeaderId int
}

type GetStateArgs struct {
	ClerkId int
}

type GetStateReply struct {
	IsLeader bool
}
