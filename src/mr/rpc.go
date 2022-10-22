package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskTp int

const (
	Map TaskTp = iota + 1
	Reduce
	WAIT
	FINISH
	WRITE
	NOTWRITE
)

type TaskReply struct {
	Tp      TaskTp
	NReduce int
	Id      int
	File    []string
	FileCnt int
}

type TaskArg struct {
	Pid int
}

type MapArg struct {
	Input  string
	Output []string
	Pid    int
}

type ReduceArg struct {
	Id     int
	Output string
	Pid    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
