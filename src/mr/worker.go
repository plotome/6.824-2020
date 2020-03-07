package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		task := TaskRequest()
		// 注意可能会出现整体任务未完成，但是没有活干的情况，
		// 这时候不能退出，使用 task.Ok 判断该情况，然后等待
		// 一段时间之后再次向 master 发出请求
		if task.TaskType == Free {
			break
		} else if !task.Ok {
			// 整体任务未完成，但是没有活干的情况
			// 等待 1s 钟后轮询
			time.Sleep(time.Second)
			continue
		}
		// 测试时暂时用延迟代替实际任务
		time.Sleep(3 * time.Second)
		TaskFinishedReport(task.File, task.TaskType)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func TaskRequest() TaskReply {

	// declare an argument structure.
	args := TaskRequestArgs{}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.ReplyTaskToWorker", &args, &reply)

	fmt.Printf("reply.File: %v, reply.TaskType: %v\n", reply.File, reply.TaskType)
	return reply
}

// 任务完成后调用
func TaskFinishedReport(file string, taskType TaskTypes) {
	args := TaskFinishedArgs{
		File:     file,
		TaskType: taskType,
	}
	reply := TaskFinishedReply{}

	call("Master.ReportFinishedTask", &args, &reply)
	fmt.Printf("reply.Ok: %v \n", reply.Ok)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
