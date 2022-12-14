package mr

import (
	"errors"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState string

const (
	StateUnStarted  = "UnStarted"
	StateProcessing = "Processing"
	StateFinished   = "Finished"
)

type MapTask struct {
	file  string
	state TaskState
}

type Coordinator struct {
	// Your definitions here.
	nReduce          int
	files            []string
	unStartedTasks   []string
	finishedTasks    map[string]bool
	processingTasks  []string
	mapTaskNumber    int
	reduceTaskNumber int
	lock             sync.Mutex
}

func newMapTask(file string) MapTask {
	return MapTask{
		file:  file,
		state: StateUnStarted,
	}
}

// Your code here -- RPC handlers for the worker to call.

// RequestMapTask mapper request map task
func (c *Coordinator) RequestMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	c.lock.Lock()
	if len(c.unStartedTasks) == 0 {
		return errors.New("no tasks")
	}
	taskNumber := c.mapTaskNumber
	unStartedTask := c.unStartedTasks[0]
	c.unStartedTasks = c.unStartedTasks[1:]
	c.processingTasks = append(c.processingTasks, reply.File)
	c.mapTaskNumber++
	c.lock.Unlock()

	reply.TaskNumber = taskNumber
	reply.File = unStartedTask
	reply.NReduce = c.nReduce
	return nil
}

// SubmitMapResult mapper pass back map phase's result to coordinator
func (c *Coordinator) SubmitMapResult(args *MapResultArgs, reply *MapResultReply) error {
	return nil
}

func (c *Coordinator) setFinished() {

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

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.unStartedTasks = files
	c.finishedTasks = make(map[string]bool)
	c.processingTasks = []string{}

	c.server()
	return &c
}
