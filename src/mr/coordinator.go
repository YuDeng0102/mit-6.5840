package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	mapFinish       int
	reduceFinish    int
	mapOk           []bool
	reduceOk        []bool
	mapInProcess    []int
	reduceInProcess []int
	activateWorkers int
	workerOk        map[int]struct{}
)

// definition of stage of a coordinator
const (
	MapStage int = iota
	ReduceStage
	MapWait
	ReduceWait
	Success
	//only for task
	Waiting
)

type Task = AssignTaskReply

type Coordinator struct {
	// Your definitions here.
	m              sync.Mutex
	taskMutex      sync.Mutex
	stage          int
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	MapNumber      int
	ReduceNumber   int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := workerOk[args.WorkerId]; !ok {
		workerOk[args.WorkerId] = struct{}{}
		activateWorkers++
	}
	switch c.stage {
	case MapStage:
		{
			if len(c.MapTaskChan) > 0 {
				*reply = *<-c.MapTaskChan
				mapInProcess[reply.TaskId] = args.WorkerId
				go func(t Task) {

					<-time.After(10 * time.Second)
					{
						c.taskMutex.Lock()
						if !mapOk[t.TaskId] {
							c.MapTaskChan <- &t
							mapInProcess[t.TaskId] = -1
							c.stage = MapStage
							log.Printf("amp time exced %v\n", t.TaskId)
						}
						c.taskMutex.Unlock()
					}
				}(*reply)
			} else {
				c.stage = MapWait
				reply.TaskType = Waiting
			}
		}
	case MapWait:
		{
			if mapFinish == c.MapNumber {
				c.MakeReduceTask()
				c.stage = ReduceStage
				log.Printf("finish map,go into reduce stage")
			}
			reply.TaskType = Waiting
		}
	case ReduceStage:
		{
			if len(c.ReduceTaskChan) > 0 {
				*reply = *<-c.ReduceTaskChan
				reduceInProcess[reply.TaskId] = args.WorkerId
				go func(t Task) {
					<-time.After(10 * time.Second)
					{
						c.taskMutex.Lock()
						if !reduceOk[t.TaskId] {
							c.ReduceTaskChan <- &t
							reduceInProcess[t.TaskId] = -1
							c.stage = ReduceStage
							log.Printf("reduce time exced %v\n", t.TaskId)
						}
						c.taskMutex.Unlock()
					}
				}(*reply)
			} else {
				c.stage = ReduceWait
				reply.TaskType = Waiting
			}
		}
	case ReduceWait:
		{
			if reduceFinish == c.ReduceNumber {
				c.stage = Success
				reply.TaskType = Success
				activateWorkers--
				log.Printf("finish all reduce tasks ,success!")
				break
			}
			reply.TaskType = Waiting
		}
	case Success:
		activateWorkers--
		reply.TaskType = Success

	}
	return nil
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, replys *TaskFinishReplys) error {
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if args.TaskType == MapStage {
		if mapInProcess[args.TaskId] != args.WorkerId {
			return nil
		}
		mapOk[args.TaskId] = true
		mapFinish++
	} else if args.TaskType == ReduceStage {
		if reduceInProcess[args.TaskId] != args.WorkerId {
			return nil
		}
		reduceOk[args.TaskId] = true
		reduceFinish++
	}
	replys.Success = true
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
	ret := false
	c.m.Lock()
	defer c.m.Unlock()
	ret = c.stage == Success && activateWorkers == 0
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MapStage,
		MapNumber:      len(files),
		ReduceNumber:   nReduce,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
	}
	mapOk = make([]bool, len(files))
	reduceOk = make([]bool, nReduce)
	mapInProcess = make([]int, len(files))
	reduceInProcess = make([]int, nReduce)
	workerOk = make(map[int]struct{})
	c.MakeMapTask(files)
	c.server()
	return &c
}

func (c *Coordinator) MakeMapTask(files []string) {
	c.taskMutex.Lock()
	for i, file := range files {
		task := Task{
			TaskType:     MapStage,
			TaskId:       i,
			ReduceNumber: c.ReduceNumber,
			FileName:     file,
		}
		mapInProcess[i] = -1
		c.MapTaskChan <- &task
	}
	c.taskMutex.Unlock()
}

func (c *Coordinator) MakeReduceTask() {
	c.taskMutex.Lock()
	for i := 0; i < c.ReduceNumber; i++ {
		task := Task{
			TaskType:     ReduceStage,
			TaskId:       i,
			ReduceNumber: c.ReduceNumber,
			FileName:     "",
		}
		reduceInProcess[i] = -1
		c.ReduceTaskChan <- &task
	}
	c.taskMutex.Unlock()
}
