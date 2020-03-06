package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatusMap map[string]int

// Master 结构体，里面应该存储 Master 的所有必要信息（废话）
type Master struct {
	// map 任务状态：UnStarted, Processing, Finished
	MapTaskStatus   TaskStatusMap
	MapTaskFinished bool

	// Reduce Task 的个数，即 nReduce
	ReduceTaskNum int

	// reduce task 状态
	ReduceTaskStatus   TaskStatusMap
	ReduceTaskFinished bool

	// master 当前任务状态
	TaskStatus int
	RWMux      *sync.RWMutex
}

const (
	Free = iota
	MapTask
	ReduceTask
)

const (
	UnStarted = iota
	Processing
	Finished
)

func InitMapTaskStatus(files []string) TaskStatusMap {
	taskStatusMap := make(TaskStatusMap)
	for _, file := range files {
		taskStatusMap[file] = UnStarted
	}
	return taskStatusMap
}

func InitReduceTaskStatus(nReduce int) TaskStatusMap {
	taskStatusMap := make(TaskStatusMap)
	for i := 0; i < nReduce; i++ {
		filename := GetReduceFileName(i)
		taskStatusMap[filename] = UnStarted
	}
	return taskStatusMap
}

func GetReduceFileName(index int) string {
	return "mr-out-" + strconv.Itoa(index)
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (master *Master) ReplyTaskToWorker(args *TaskRequestArgs, reply *TaskReply) error {
	file, ok := master.GetUnStartedTask()
	reply.File = file
	reply.Ok = ok
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
// 如果整个任务完成，返回 true
func (master *Master) Done() bool {
	// change ret := false to true so that the master exits immediately
	ret := false
	//ret := true
	// Your code here.

	return ret
}

// 从 map/reduce 任务中获取一个没有开始的任务
func (master *Master) GetUnStartedTask() (string, bool) {
	master.RWMux.Lock()
	defer master.RWMux.Unlock()
	if master.TaskStatus == MapTask {
		for k, v := range master.MapTaskStatus {
			if v == UnStarted {
				master.MapTaskStatus[k] = Processing
				go master.TimeOutDetection(k, MapTask)
				return k, true
			}
		}
	}
	if master.TaskStatus == ReduceTask {
		for k, v := range master.ReduceTaskStatus {
			if v == UnStarted {
				master.ReduceTaskStatus[k] = Processing
				go master.TimeOutDetection(k, ReduceTask)
				return k, true
			}
		}
	}
	return "", false
}

// 超时检测任务，如果 10s 后任务还是 processing 状态，就认为
// worker 故障，直接将任务设为 UnStarted 状态
//
func (master *Master) TimeOutDetection(file string, taskType int) {
	time.Sleep(10 * time.Second)
	master.RWMux.Lock()
	defer master.RWMux.Unlock()
	if taskType == MapTask {
		if master.MapTaskStatus[file] == Processing {
			fmt.Println("The worker of Map %s has faild", file)
			master.MapTaskStatus[file] = UnStarted
		}
		return
	}

	if taskType == ReduceTask {
		if master.ReduceTaskStatus[file] == Processing {
			fmt.Printf("The worker of Reduce %v has faild", file)
			master.ReduceTaskStatus[file] = UnStarted
		}
		return
	}
}

//
// start a thread that listens for RPCs from worker.go
// 创建一个线程，监听 worker 的 RPC 请求
func (master *Master) server() {
	rpc.Register(master)
	rpc.HandleHTTP()
	//listener, err := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listener, nil)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// 创建具有 n 个 reduce 任务的 master
//
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化 master
	master := Master{
		MapTaskStatus:    InitMapTaskStatus(files),
		ReduceTaskNum:    nReduce,
		ReduceTaskStatus: InitReduceTaskStatus(nReduce),
		RWMux:            &sync.RWMutex{},
		TaskStatus:       MapTask,
	}

	//

	master.server()
	return &master
}
