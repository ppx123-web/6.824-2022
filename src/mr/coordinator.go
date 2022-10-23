package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

/*
		Worker 						Coordinator
		----> 	AskForTask 			---->
		<---- 	assign Map task 	<----
		----> 	ReduceMap	     	---->
		<----	Check Can Write		<----
CanWrite----> 	FinishMap			---->
either	----> 	abort

		----> 	AskForTask		 	---->
		<---- 	assign Reduce task 	<----
		----> 	ReduceWrite	     	---->
		<----	Check Can Write		<----
CanWrite---->	FinishReduce		---->
either	---->	abort

And c.CheckFail() check whether running workers delay or crash
To protect from data racing, use lock (chan int) to protect map

*/

type FileState int

const (
	NEW FileState = iota + 1
	OLD
	USING
)

type UsingInfo struct {
	Time int
	Id   int
	Part int
	flag bool
	Pid  int
}

type Lock chan int

type Coordinator struct {
	Nreduce                 int
	MapFinish, ReduceFinish int
	ReduceCnt, MapCnt       chan int
	FileCnt                 int
	Files                   []string
	MapFile                 map[string]UsingInfo
	ReduceFile              map[int]UsingInfo
	Reduces                 []string
	MapLock                 Lock
}

func (lk Lock) lock() {
	val := <-lk
	if val != 1 {
		panic("lock error")
	}
}

func (lk Lock) unlock() {
	lk <- 1
}

const CrashTime = 10

// a loop to check whether there are some workers timeout or crash per second
func (c *Coordinator) CheckFail() {
	for {
		t := int(time.Now().Unix())
		mapid := []int{}
		reducelist := []int{}
		c.MapLock.lock()
		for _, v := range c.MapFile {
			if t-v.Time >= CrashTime {
				mapid = append(mapid, v.Id)
				v.flag = false
			}
		}
		for k, v := range c.ReduceFile {
			if t-v.Time > CrashTime {
				reducelist = append(reducelist, k)
				v.flag = false
			}
		}

		for _, id := range mapid {
			c.MapCnt <- id
		}

		for _, id := range reducelist {
			c.ReduceCnt <- id
		}
		if c.MapFinish == c.FileCnt && c.ReduceFinish == c.Nreduce {
			c.MapLock.unlock()
			break
		} else {
			c.MapLock.unlock()
			time.Sleep(time.Second)
		}
	}

}

// RPC handlers for the worker to call.

// handle request for task
func (c *Coordinator) AskForTask(args *TaskArg, reply *TaskReply) error {
	c.MapLock.lock()
	if len(c.MapCnt) > 0 {
		id := <-c.MapCnt
		file := c.Files[id]

		reply.Tp = Map
		reply.File = []string{file}
		reply.NReduce = c.Nreduce
		reply.Id = id + 10
		// fmt.Printf("Coordinator:Assign Map %v %v\n", id, file)
		c.MapFile[file] = UsingInfo{Time: int(time.Now().Unix()), Part: -1, Id: id, flag: true, Pid: args.Pid}
	} else if len(c.ReduceCnt) > 0 && c.MapFinish == c.FileCnt {
		id := <-c.ReduceCnt
		reply.Tp = Reduce
		reply.Id = id + 10
		reply.FileCnt = c.FileCnt
		reply.NReduce = c.Nreduce
		reply.File = []string{fmt.Sprintf("intermediate-%v", id)}

		c.ReduceFile[id] = UsingInfo{Time: int(time.Now().Unix()), Part: id, Id: id, flag: true, Pid: args.Pid}
	} else if c.MapFinish == c.FileCnt && c.ReduceFinish == c.Nreduce {
		reply.Tp = FINISH
	} else {
		reply.Tp = WAIT
	}
	c.MapLock.unlock()
	return nil
}

// Tell Worker to write or abort by checking the Pid
func (c *Coordinator) MapWrite(args *MapArg, reply *TaskReply) error {
	c.MapLock.lock()
	// reply.Tp = NOTWRITE
	if info, ok := c.MapFile[args.Input]; ok {
		if info.flag && info.Pid == args.Pid {
			info.flag = false
			c.MapFile[args.Input] = info
			reply.Tp = WRITE
			//cannot assign to struct field c.MapFile[args.Input].flag in map
			// fmt.Printf("Coordinator:Map Finish %v\n", args.Input)
		} else {
			// fmt.Printf("Coordinator:Map Wrong Pid or flag abort %v\n", args.Input)
			reply.Tp = NOTWRITE
		}
	} else {
		// fmt.Printf("Coordinator:Map No MapFile abort %v\n", args.Input)
		reply.Tp = NOTWRITE
	}
	c.MapLock.unlock()
	return nil
}

// A worker finishes Map
func (c *Coordinator) FinishMap(args *MapArg, reply *TaskReply) error {
	c.MapLock.lock()
	if c.MapFile[args.Input].Pid == args.Pid {
		delete(c.MapFile, args.Input)
		c.MapFinish++
		// if c.MapFinish == c.FileCnt {
		// 	fmt.Printf("Map All Finish\n")
		// }
	}
	c.MapLock.unlock()
	return nil
}

// Tell Worker to write or abort by checking the Pid
func (c *Coordinator) ReduceWrite(args *ReduceArg, reply *TaskReply) error {
	c.MapLock.lock()
	if item, ok := c.ReduceFile[args.Id]; ok {
		if item.flag && item.Pid == args.Pid {
			reply.Tp = WRITE
			item.flag = false
			c.ReduceFile[args.Id] = item
			//cannot assign to struct field c.ReduceFile[args.Id].flag in map
		} else {
			reply.Tp = NOTWRITE
		}
	} else {
		reply.Tp = NOTWRITE
	}
	c.MapLock.unlock()
	return nil
}

// A worker finishes Reduce
func (c *Coordinator) FinishReduce(args *ReduceArg, reply *TaskReply) error {
	c.MapLock.lock()
	if _, ok := c.ReduceFile[args.Id]; ok && args.Pid == c.ReduceFile[args.Id].Pid {
		delete(c.ReduceFile, args.Id)
		c.ReduceFinish++
	}
	c.MapLock.unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for {
		time.Sleep(time.Second)
		c.MapLock.lock()
		if c.ReduceFinish == c.Nreduce {
			c.MapLock.unlock()
			time.Sleep(time.Second)
			return true
		}
		c.MapLock.unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Nreduce:      nReduce,
		ReduceCnt:    make(chan int, nReduce),
		MapCnt:       make(chan int, nReduce),
		FileCnt:      len(files),
		MapFile:      make(map[string]UsingInfo),
		ReduceFile:   make(map[int]UsingInfo),
		Files:        files[:],
		Reduces:      []string{},
		MapFinish:    0,
		ReduceFinish: 0,
		MapLock:      make(chan int, 1),
	}
	c.MapLock <- 1

	for i := 0; i < nReduce; i++ {
		c.ReduceCnt <- i
	}
	for i := range files {
		c.MapCnt <- i
	}
	go c.CheckFail()
	c.server()
	return &c
}
