package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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
			reduceWorker(taskInfo.ReduceId, reducef)
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

func reduceWorker(reduceID int, reducef func(string, []string) string) {
	filePattern := fmt.Sprintf("mr-*-%v", reduceID)
	files, err := filepath.Glob(filePattern)
	if err != nil {
		log.Fatal(err)
	}

	intermediate := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("open file %s fail: %v", filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		kva := []KeyValue{}
		if err := dec.Decode(&kva); err != nil {
			log.Printf("decode file %s fail: %v", filename, err)
			break
		}
		intermediate = append(intermediate, kva...)
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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

func CallDoneTask(taskInfo *TaskInfo) {
	args := DoneTaskArgs{
		TaskInfo: taskInfo,
	}
	reply := DoneTaskReply{}
	args.TaskInfo = taskInfo
	ok := call("Coordinator.DoneTask", &args, &reply)
	if ok {
		fmt.Printf("Done task call succes %v\n", taskInfo)
	} else {
		fmt.Printf("call failed! \n")
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
