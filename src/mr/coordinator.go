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
	Files                   chan string
	MapFile                 map[string]UsingInfo
	ReduceFile              map[int]UsingInfo
	Reduces                 []string
	MapLock                 Lock
}

func (lk *Lock) lock() {
	val := <-*lk
	if val != 1 {
		panic("lock error")
	}
}

func (lk *Lock) unlock() {
	*lk <- 1
}

const CrashTime = 10

func (c *Coordinator) CheckFail() {
	for {
		t := int(time.Now().Unix())
		mapfile := []string{}
		mapid := []int{}
		reducelist := []int{}
		c.MapLock.lock()
		for k, v := range c.MapFile {
			if t-v.Time >= CrashTime {
				mapfile = append(mapfile, k)
				mapid = append(mapid, v.Id)
			}
		}
		for k, v := range c.ReduceFile {
			if t-v.Time > CrashTime {
				reducelist = append(reducelist, k)
			}
		}
		c.MapLock.unlock()

		for i, f := range mapfile {
			c.Files <- f
			c.MapCnt <- mapid[i]
		}

		for _, id := range reducelist {
			c.ReduceCnt <- id
		}

		if c.MapFinish == c.FileCnt && c.ReduceFinish == c.Nreduce {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

}

// Your code here -- RPC handlers for the worker to call.

func (m *Coordinator) AskForTask(args *TaskArg, reply *TaskReply) error {
	if len(m.Files) > 0 {
		file := <-m.Files
		id := <-m.MapCnt

		reply.Tp = Map
		reply.File = []string{file}
		reply.NReduce = m.Nreduce
		reply.Id = id

		m.MapLock.lock()
		m.MapFile[file] = UsingInfo{Time: int(time.Now().Unix()), Part: -1, Id: id, flag: true, Pid: args.Pid}
		m.MapLock.unlock()
	} else if len(m.ReduceCnt) > 0 && m.MapFinish == m.FileCnt {
		id := <-m.ReduceCnt

		reply.Tp = Reduce
		reply.Id = id + 10
		reply.FileCnt = m.FileCnt
		reply.NReduce = m.Nreduce
		reply.File = []string{fmt.Sprintf("intermediate-%v", id)}
		m.MapLock.lock()
		m.ReduceFile[id] = UsingInfo{Time: int(time.Now().Unix()), Part: id, Id: id, flag: true, Pid: args.Pid}
		m.MapLock.unlock()
	} else if m.MapFinish == m.FileCnt && m.ReduceFinish == m.Nreduce {
		reply.Tp = FINISH
	} else {
		reply.Tp = WAIT
	}
	return nil
}

func (m *Coordinator) MapWrite(args *MapArg, reply *TaskReply) error {
	m.MapLock.lock()
	defer m.MapLock.unlock()
	if info, ok := m.MapFile[args.Input]; ok {
		if m.MapFile[args.Input].flag && info.Pid == args.Pid {
			info.flag = false
			m.MapFile[args.Input] = info
			reply.Tp = WRITE
		} else {
			reply.Tp = NOTWRITE
		}
	} else {
		reply.Tp = NOTWRITE
	}
	return nil
}

func (m *Coordinator) FinishMap(args *MapArg, reply *TaskReply) error {
	m.MapLock.lock()
	defer m.MapLock.unlock()
	if m.MapFile[args.Input].Pid == args.Pid {
		delete(m.MapFile, args.Input)
		m.MapFinish++
	}
	return nil
}

func (m *Coordinator) ReduceWrite(args *ReduceArg, reply *TaskReply) error {
	m.MapLock.lock()
	defer m.MapLock.unlock()
	if item, ok := m.ReduceFile[args.Id]; ok {
		if item.flag {
			reply.Tp = WRITE
			item.flag = false
			m.ReduceFile[args.Id] = item
		} else {
			reply.Tp = NOTWRITE
		}
	} else {
		reply.Tp = NOTWRITE
	}
	return nil
}

func (m *Coordinator) FinishReduce(args *ReduceArg, reply *TaskReply) error {
	m.MapLock.lock()
	defer m.MapLock.unlock()
	if _, ok := m.ReduceFile[args.Id]; ok {
		delete(m.ReduceFile, args.Id)
		m.ReduceFinish++
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

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
		time.Sleep(1 * time.Second)
		if c.ReduceFinish == c.Nreduce {
			return true
		}
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
		Files:        make(chan string, len(files)),
		Reduces:      []string{},
		MapFinish:    0,
		ReduceFinish: 0,
		MapLock:      make(chan int, 1),
	}
	c.MapLock <- 1

	for i := 0; i < nReduce; i++ {
		c.ReduceCnt <- i
	}
	for i, f := range files {
		c.MapCnt <- i
		c.Files <- f
	}
	go c.CheckFail()
	c.server()
	return &c
}
