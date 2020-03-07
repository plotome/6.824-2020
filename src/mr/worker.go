package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

// for sorting by key.
// Array of Key-Value pairs
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

		// 正式处理过程
		if task.TaskType == MapTask {
			MapTaskProcess(task, mapf)
		} else if task.TaskType == ReduceTask {
			ReduceTaskProcess(task, reducef)
		}
		// 测试时暂时用延迟代替实际任务
		// time.Sleep(3 * time.Second)
		TaskFinishedReport(task.File, task.TaskType)
	}

}

// Reduce 阶段处理过程
func ReduceTaskProcess(task TaskReply,
	reducef func(string, []string) string) {

	// nReduce := task.ReduceTaskNum
	// fileName := task.File
	taskId := task.TaskId
	mapTaskNum := task.MapTaskNum
	// intermediate 已排序
	intermediate := MergeInterFiles(mapTaskNum, taskId)

	oname := "mr-out-" + strconv.Itoa(taskId)
	WriteReduceResult(oname, intermediate, reducef)
}

// 调用 reducef 处理 kv 键值对，并将结果写入文件 oname
func WriteReduceResult(oname string,
	intermediate []KeyValue,
	reducef func(string, []string) string) {

	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// call the reduce procedure
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	fmt.Printf("Reduce task : %v completed \n", oname)
}

func MergeInterFiles(mapTaskNum int, taskId int) []KeyValue {
	kva := []KeyValue{}
	for i := 0; i < mapTaskNum; i++ {
		// 将 kv 键值对从 tempfiles 里全部读出来
		tmpfile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskId)
		kva = ReadFromJSONFile(kva, tmpfile)
	}

	// 排序后返回（此时不需要写入到文件）
	sort.Sort(ByKey(kva))
	// oname := "mr-out-" + strconv.Itoa(taskId)
	// WriteToJSONFile(kva, oname)
	return kva
}

func ReadFromJSONFile(kva []KeyValue, tmpfile string) []KeyValue {
	file, _ := os.Open(tmpfile)
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// Map 阶段处理过程
func MapTaskProcess(task TaskReply,
	mapf func(string, string) []KeyValue) {

	nReduce := task.ReduceTaskNum
	fileName := task.File
	taskId := task.TaskId

	content := LoadFileContent(fileName)

	// kva 为文件内所有 key-value 键值对
	kva := mapf(fileName, string(content))

	kvas := PartitionByKey(kva, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(i)
		WriteToJSONFile(kvas[i], fileName)
	}
	fmt.Printf("Creat mr-temp files completed. \n")
}

// 将KeyValue 键值对按 Key 分到 nReduce 个分片里
func PartitionByKey(kva []KeyValue, nReduce int) [][]KeyValue {
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		kvas[idx] = append(kvas[idx], kv)
	}
	return kvas
}

// 将文件写入到磁盘
func WriteToJSONFile(intermediate []KeyValue, fileName string) (string, bool) {
	// jsonFile, _ := ioutil.TempFile("./", fileName)
	// 如果文件存在则清空文件
	jsonFile, _ := os.Create(fileName)
	defer jsonFile.Close() //关闭文件，释放资源
	enc := json.NewEncoder(jsonFile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("error: ", err)
			return fileName, false
		}
	}
	fmt.Printf("File : %v has saved\n", fileName)
	return fileName, true
}

// 判断文件是否存在
func IsFileExist(fileName string) bool {
	_, err := os.Lstat(fileName)
	return !os.IsNotExist(err)
}

// 将文件内容加载到内存中，便于后续使用
func LoadFileContent(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return content
}

// 向 master 发送 task 请求
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
