package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/samber/lo"
)

const (
	TempFileRootPath = "./"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// read a task from the coordinator.
		task := getTask()
		if task == nil {
			break
		}
		switch task.TaskType {
		case TaskNone:
			myLog("worker got a TaskNone task")
			// do nothing.
			return
		case TaskSleep:
			time.Sleep(WorkerSleepDuration)
		case TaskMap:
			doMapTask(task, mapf)
			taskReport(task)
		case TaskReduce:
			doReduceTask(task, reducef)
			taskReport(task)
		}
	}
}

func taskReport(taskInfo *TaskInfo) {
	myLog("worker report task: %+v\n", taskInfo)
	ok := call("Coordinator.TaskReport", taskInfo, taskInfo)
	if !ok {
		panic("task report error")
	}
}

func doMapTask(taskInfo *TaskInfo, mapf func(string, string) []KeyValue) (res []string) {
	data, err := readFile(taskInfo, taskInfo.MapTaskFilePath)
	if err != nil {
		panic(fmt.Sprintf("read file error: %s", err))
	}
	kvArr := mapf(taskInfo.MapTaskFilePath, string(data))
	fileMap := map[string][]*KeyValue{}
	for _, kv := range kvArr {
		index := fmt.Sprintf("%d", ihash(kv.Key)%taskInfo.NReduce)
		filename := taskInfo.GetFilename(index)
		tmpKv := kv
		fileMap[filename] = append(fileMap[filename], &tmpKv)
	}
	for k, arr := range fileMap {
		sort.Slice(fileMap[k], func(i, j int) bool {
			return arr[i].Key < arr[j].Key
		})
	}
	for filename, arr := range fileMap {
		saveFile(filename, arr, TaskMap)
		res = append(res, filename)
	}
	sort.Strings(res)
	taskInfo.ReduceTaskFilePath = res
	return res
}

func doReduceTask(taskInfo *TaskInfo, reducef func(string, []string) string) {
	m := map[string][]string{}
	myLog("do reduce task: %+v\n", taskInfo)
	for _, filepath := range taskInfo.ReduceTaskFilePath {
		file, err := os.Open(filepath)
		defer file.Close()
		if err != nil {
			myLog(err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
	}
	reduceMap := map[string]string{}
	for k, v := range m {
		reduceMap[k] = reducef(k, v)
	}
	res := lo.MapToSlice(reduceMap, func(key string, value string) *KeyValue {
		return &KeyValue{
			Key:   key,
			Value: value,
		}
	})
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})

	filename := taskInfo.GetFilename("")
	saveFile(filename, res, TaskReduce)

	taskInfo.ReduceOutPutFilePath = filename
}

func readFile(taskInfo *TaskInfo, filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("open file error: %s, task info: %+v", err, taskInfo))
		return nil, err
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			panic(fmt.Sprintf("close file error: %s", err))
		}
	}(file)
	data, err := io.ReadAll(file)
	if err != nil {
		panic(fmt.Sprintf("read file error: %s, task info: %+v", err, taskInfo))
		return nil, err
	}
	return data, err
}

func saveFile(fileName string, kvArr []*KeyValue, taskType TypeTask) {
	tmpFile, err := os.CreateTemp(TempFileRootPath, fileName)
	if err != nil {
		panic(tmpFile)
	}
	defer func(oldPath, newPath string) {
		err = os.Rename(oldPath, newPath)
		if err != nil {
			panic(err)
		}
	}(tmpFile.Name(), fileName)
	switch taskType {
	case TaskMap:
		enc := json.NewEncoder(tmpFile)
		for _, kv := range kvArr {
			err = enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
	case TaskReduce:
		for _, kv := range kvArr {
			fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
		}
	}
}

func getTask() *TaskInfo {
	taskInfo := &TaskInfo{}
	ok := call("Coordinator.GetTask", "", taskInfo)
	myLog("Worker get task: %+v\n", taskInfo)
	if ok {
		return taskInfo
	} else {
		panic("get task info error")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
