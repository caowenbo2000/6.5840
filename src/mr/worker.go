package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		taskInfo := CallGetTask()
		if taskInfo == nil {
			continue
		}

		if taskInfo.TaskType == MapTask {
			mapWorker(taskInfo.FileName, mapf, taskInfo.NReduce)
		}
		if taskInfo.TaskType == ReduceTask {
			reduceWorker(taskInfo.ReduceId)
		}

	}
}

func mapWorker(fileName string, mapf func(string, string) []KeyValue, nReduce int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	kvaFileWrite(kva, nReduce, fileName)
}

func kvaFileWrite(kva []KeyValue, nReduce int, fileName string) {
	mapkva := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		mapkva[reduceID] = append(mapkva[reduceID], kv)
	}
	for reduceID, kva := range mapkva {
		sort.Sort(ByKey(kva))

		processFileName := fmt.Sprintf("mr-%v-%v-temp", fileName, reduceID)
		file, err := os.Create(processFileName)
		if err != nil {
			log.Fatalf("cannot create %v", processFileName)
		}

		enc := json.NewEncoder(file)
		enc.Encode(kva)
		file.Close()
		os.Rename(processFileName, fmt.Sprintf("mr-%v-%v", fileName, reduceID))
	}
}

func reduceWorker(reduceID int) {

}

func CallGetTask() *TaskInfo {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		fmt.Printf("reply.TaskInfo %v\n", reply.TaskInfo)
	} else {
		fmt.Printf("call failed! \n")
	}
	return reply.TaskInfo
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
