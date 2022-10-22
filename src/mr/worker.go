package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var reply TaskReply
	var task TaskArg
	task.Pid = os.Getpid()
	for {
		call("Coordinator.AskForTask", &task, &reply)
		if reply.Tp == Map {
			// fmt.Printf("Get Map Task %v\n", os.Getpid())
			ret := MapWork(mapf, &reply)
			ret.Pid = task.Pid
			// fmt.Printf("Finish Map Task\n")
			if len(ret.Input) > 0 {
				call("Coordinator.FinishMap", &ret, &reply)
			}
		} else if reply.Tp == Reduce {
			reply.Id -= 10
			// fmt.Printf("Get Reduce Task %v\n", os.Getpid())
			ret := ReduceWork(reducef, &reply)
			ret.Pid = task.Pid
			// fmt.Printf("Finish Reduce Task\n")
			if ret.Id != -1 {
				call("Coordinator.FinishReduce", &ret, &reply)
			}
		} else if reply.Tp == WAIT {
			time.Sleep(time.Second * 1)
		} else if reply.Tp == FINISH {
			break
			// quit when finish
		}
	}

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func MapWork(mapf func(string, string) []KeyValue, arg *TaskReply) MapArg {
	var intermediate []KeyValue

	filename := arg.File[0]
	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	values := make([]map[string][]string, arg.NReduce)
	for i := 0; i < arg.NReduce; i++ {
		values[i] = make(map[string][]string)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		str := intermediate[i].Key
		index := ihash(str) % arg.NReduce
		for k := i; k < j; k++ {
			values[index][str] = append(values[index][str], intermediate[k].Value)
		}
		i = j
	}
	var reply TaskReply
	call("Coordinator.MapWrite", &MapArg{Input: arg.File[0], Pid: os.Getpid()}, &reply)
	if reply.Tp == WRITE {
		for i, v := range values {
			WriteJson(fmt.Sprintf("mr-%v-%v", arg.Id, i), v)
		}
		return MapArg{Input: arg.File[0]}
	} else {
		return MapArg{Input: ""}
	}

}

func ReduceWork(reducef func(string, []string) string, arg *TaskReply) ReduceArg {
	res := make(map[string]string)
	all := make(map[string][]string)
	for i := 0; i < arg.FileCnt; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, arg.Id)
		values := ReadJson(filename)
		os.Remove(filename)
		for k, v := range values {
			all[k] = append(all[k], v...)
		}
	}

	for k, v := range all {
		res[k] += reducef(k, v)
	}
	var reply TaskReply
	call("Coordinator.ReduceWrite", &ReduceArg{Id: arg.Id, Pid: os.Getpid()}, &reply)
	if reply.Tp == WRITE {
		str := fmt.Sprintf("mr-out-%v", arg.Id)
		outputfile, _ := os.OpenFile(str, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
		for k, v := range res {
			fmt.Fprintf(outputfile, "%v %v\n", k, v)
		}
		outputfile.Close()
		return ReduceArg{Id: arg.Id}
	} else {
		return ReduceArg{Id: -1}
	}

}

// send an RPC request to the Coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func ReadJson(filename string) map[string][]string {
	values := make(map[string][]string)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Error Open " + filename + "\n")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("Error Read " + filename + "\n")
	}
	file.Close()
	json.Unmarshal(content, &values)
	return values
}

func WriteJson(filename string, values map[string][]string) {
	output, _ := json.Marshal(values)
	f, _ := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	fmt.Fprintf(f, string(output))
	f.Close()
}
