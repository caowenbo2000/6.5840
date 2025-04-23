package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type taskStage int

const (
	MapStage taskStage = iota
	ReduceStage
	DoneStage
)

type taskStatus int

const (
	TaskInit taskStatus = iota
	TaskRunning
	TaskDone
)

type taskInfo struct {
	taskStatus taskStatus
	startTime  int64
}

type Coordinator struct {
	taskStage    taskStage
	mapStatus    map[string]*taskInfo
	reduceStatus map[int]*taskInfo
	mutex        sync.Mutex
}

const taskTimeOut = int64(10)

func CoordinatorInit(c *Coordinator, files []string, nReduce int) {
	c.taskStage = MapStage
	c.mapStatus = make(map[string]*taskInfo)
	c.reduceStatus = make(map[int]*taskInfo)
	for _, v := range files {
		c.mapStatus[v] = &taskInfo{TaskInit, 0}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = &taskInfo{TaskInit, 0}
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.taskStage == MapStage {
		hasRunningTask := false
		for k, v := range c.mapStatus {
			if v.taskStatus == TaskRunning {
				hasRunningTask = true
			}
			if v.taskStatus == TaskInit ||
				v.taskStatus == TaskRunning && time.Now().Unix() > taskTimeOut+v.startTime {
				reply.TaskInfo = TaskInfo{MapTask, k, 0, 0}
				return
			}
		}
		if hasRunningTask {
			reply.TaskInfo = TaskInfo{WaitTask, "", 0, 1}
			return
		} else {
			c.taskStage = ReduceStage
		}
	}

	if c.taskStage == ReduceStage {
		hasRunningTask := false
		for k, v := range c.reduceStatus {
			if v.taskStatus == TaskRunning {
				hasRunningTask = true
			}
			if v.taskStatus == TaskInit ||
				v.taskStatus == TaskRunning && time.Now().Unix() > taskTimeOut+v.startTime {
				reply.TaskInfo = TaskInfo{ReduceTask, "", k, 0}
				return
			}
		}
		if hasRunningTask {
			reply.TaskInfo = TaskInfo{WaitTask, "", 0, 1}
			return
		} else {
			c.taskStage = DoneStage
			reply.TaskInfo = TaskInfo{DoneTask, "", 0, 0}
			return
		}
	}

	if c.taskStage == DoneStage {
		reply.TaskInfo = TaskInfo{DoneTask, "", 0, 0}
		return
	}
	return
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) {
	reply = new(DoneTaskReply)
	if args == nil {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskInfo.TaskType == MapTask {
		c.mapStatus[args.TaskInfo.FileName].taskStatus = TaskDone
	} else if args.TaskInfo.TaskType == ReduceTask {
		c.reduceStatus[args.TaskInfo.ReduceId].taskStatus = TaskDone
	}

	return
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

// main/mrcoordinator.go calls TaskDone() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.taskStage == DoneStage
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	CoordinatorInit(&c, files, nReduce)
	c.server()
	return &c
}
