package mr

import (
	"fmt"
	"io/ioutil"
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

type TaskState int

const (
	Running TaskState = iota + 1
	GetWritePerm
	FinishWrite
	Dead
	WaitForAlloc
)

func TransitionToDead(item *UsingInfo) {
	if item.State != WaitForAlloc && item.State != FinishWrite {
		item.State = Dead
	}
}

type UsingInfo struct {
	State TaskState
	Time  int
	Id    int
	Part  int
	Pid   int
}

type Lock chan int

type Coordinator struct {
	Nreduce                 int
	MapFinish, ReduceFinish int
	ReduceCnt, MapCnt       chan int
	FileCnt                 int
	Files                   []string
	MapFile                 map[string]*UsingInfo
	ReduceFile              map[int]*UsingInfo
	Reduces                 []string
	MapLock                 Lock
	logger                  *log.Logger
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
	//当一个work已经进行MapWrite后，checkfail进行，将其标记加入队列，但是最终仍然可以进行FinishMap操作，而同一个任务进行了两次
	//Write同理
	for {
		t := int(time.Now().Unix())
		c.MapLock.lock()
		for _, v := range c.MapFile {
			if v.State == Dead || (t-v.Time >= CrashTime && v.State == Running) {
				c.MapCnt <- v.Id
				v.State = WaitForAlloc
				c.logger.Printf("map %v %v fail\n", v.Id, c.Files[v.Id])
			}
		}
		for k, v := range c.ReduceFile {
			if v.State == Dead || (t-v.Time >= CrashTime && v.State == Running) {
				c.ReduceCnt <- k
				v.State = WaitForAlloc
				c.logger.Printf("reduce %v fail\n", v.Id)
			}
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
		c.logger.Printf("Assign Map %v %v\n", id, file)
		if item, ok := c.MapFile[file]; ok && item.State != WaitForAlloc {
			c.logger.Printf("Assign Wrong Map Task, %v % v %v\n", id, file, item.State)
			os.Exit(1)
		}
		c.MapFile[file] = &UsingInfo{Time: int(time.Now().Unix()), Part: -1, Id: id, State: Running, Pid: args.Pid}
	} else if len(c.ReduceCnt) > 0 && c.MapFinish == c.FileCnt {
		id := <-c.ReduceCnt
		reply.Tp = Reduce
		reply.Id = id + 10
		reply.FileCnt = c.FileCnt
		reply.NReduce = c.Nreduce
		reply.File = []string{fmt.Sprintf("mr-%v", id)}
		c.logger.Printf("Assign reduce %v\n", id)
		if item, ok := c.ReduceFile[id]; ok && item.State != WaitForAlloc {
			c.logger.Printf("Assign Wrong Reduce Task, %v %v\n", id, item.State)
			os.Exit(1)
		}
		c.ReduceFile[id] = &UsingInfo{Time: int(time.Now().Unix()), Part: id, Id: id, State: Running, Pid: args.Pid}
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
	c.logger.Printf("Get Map Write Request %v %v\n", args.Input, args.Id)
	if item, ok := c.MapFile[args.Input]; ok {
		if item.State == Running && item.Pid == args.Pid && item.Id == args.Id {
			item.State = GetWritePerm
			reply.Tp = WRITE
			c.logger.Printf("Map Ask For Write %v\n", args.Input)
		} else {
			c.logger.Printf("Refuse Map Write %v, state=%v\n", args.Input, item.State)
			TransitionToDead(item)
			reply.Tp = NOTWRITE
		}
	} else {
		TransitionToDead(item)
		reply.Tp = NOTWRITE
		c.logger.Printf("Refuse Map Write %v, task doesn't exist\n", args.Input)
	}
	c.MapLock.unlock()
	return nil
}

// A worker finishes Map
func (c *Coordinator) FinishMap(args *MapArg, reply *TaskReply) error {
	c.MapLock.lock()
	c.logger.Printf("Get Map Finish Request %v %v\n", args.Input, args.Id)
	if item, ok := c.MapFile[args.Input]; ok {
		if item.State == GetWritePerm && item.Pid == args.Pid && item.Id == args.Id {
			item.State = FinishWrite
			c.MapFinish++
			c.logger.Printf("Map Finish %v, left %v\n", args.Input, c.FileCnt-c.MapFinish)
			if c.MapFinish == c.FileCnt {
				c.logger.Printf("Map All Finish\n")
			}
		} else {
			c.logger.Printf("Refuse Map Finish, %v, state=%v\n", args.Id, item.State)
			TransitionToDead(item)
		}
	} else {
		c.logger.Printf("Refuse Map Finish %v, task doesn't exist\n", args.Id)
		TransitionToDead(item)
	}

	c.MapLock.unlock()
	return nil
}

// Tell Worker to write or abort by checking the Pid
func (c *Coordinator) ReduceWrite(args *ReduceArg, reply *TaskReply) error {
	c.MapLock.lock()
	if item, ok := c.ReduceFile[args.Id]; ok {
		if item.State == Running && item.Pid == args.Pid && args.Id == item.Id {
			reply.Tp = WRITE
			item.State = GetWritePerm
			c.logger.Printf("Reduce Ask For Write %v\n", args.Id)
		} else {
			reply.Tp = NOTWRITE
			TransitionToDead(item)
			c.logger.Printf("Refuse reduce Write %v, state=%v\n", args.Id, item.State)
		}
	} else {
		reply.Tp = NOTWRITE
		TransitionToDead(item)
		c.logger.Printf("Refuse reduce Write %v, task doesn't exist\n", args.Id)
	}
	c.MapLock.unlock()
	return nil
}

// A worker finishes Reduce
func (c *Coordinator) FinishReduce(args *ReduceArg, reply *TaskReply) error {
	c.MapLock.lock()
	if item, ok := c.ReduceFile[args.Id]; ok && item.State == GetWritePerm && args.Pid == item.Pid && item.Id == args.Id {
		item.State = FinishWrite
		c.ReduceFinish++
		c.logger.Printf("Reduce Finish %v, left %v\n", args.Id, c.Nreduce-c.ReduceFinish)
	} else {
		TransitionToDead(item)
		c.logger.Printf("Refuse Reduce Finish %v, task doesn't exist or Wrong info %v\n", args.Id, item.State)
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
		MapFile:      make(map[string]*UsingInfo),
		ReduceFile:   make(map[int]*UsingInfo),
		Files:        files[:],
		Reduces:      []string{},
		MapFinish:    0,
		ReduceFinish: 0,
		MapLock:      make(chan int, 1),
		logger:       log.New(ioutil.Discard, "", log.Lshortfile),
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
