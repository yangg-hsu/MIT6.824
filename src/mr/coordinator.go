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

type MapTasksCoordinator struct {
	mmu sync.Mutex

	MapTasks                []TaskInfo
	PendingMapTasks         map[int]TaskInfo
	MapTaskTimeoutThreshold time.Duration
	MapTaskDone             bool
}

type ReduceTasksCoordinator struct {
	rmu sync.Mutex

	ReduceTasks                []TaskInfo
	PendingRedcueTasks         map[int]TaskInfo
	ReduceTaskTimeoutThreshold time.Duration
	ReduceTaskDone  bool
}

type Coordinator struct {
	// Your definitions here.

	MapTasksCoordinator
	ReduceTasksCoordinator
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.rmu.Lock()
	ret := c.ReduceTaskDone
	c.rmu.Unlock()

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	// prepare map tasks
	for idx, fname := range files {
		c.MapTasks = append(c.MapTasks, TaskInfo{
			DMapTask, idx, nReduce, time.Time{}, fname, false,
		})
	}

	// prepare reduce tasks
	for idx := 0; idx < nReduce; idx++ {
		c.ReduceTasks = append(c.ReduceTasks, TaskInfo{
			DReduceTask, idx, nReduce, time.Time{}, "", false,
		})
	}

	c.MapTaskTimeoutThreshold = time.Second * 10
	c.ReduceTaskTimeoutThreshold = time.Second * 10
	c.PendingMapTasks = make(map[int]TaskInfo)
	c.PendingRedcueTasks = make(map[int]TaskInfo)

	c.server()
	return &c
}


func (c *Coordinator) DistributeTask(emptyValue *int, reply *TaskInfo) error {

	task := TaskInfo{}
	
	c.mmu.Lock()	
	isMapTaskDone := c.MapTaskDone
	c.mmu.Unlock()

	if !isMapTaskDone {
		// deal with timeout tasks
		c.mmu.Lock()
		c.DealMapTaskTimeout()
		if c.MapTaskLen() > 0 {
			task = c.PopMapTask()
			task.TaskStartTime = time.Now() // update task start time
			c.PendingMapTasks[task.TaskId] = task
		} else if c.PendingMapTasksLen() > 0 { // In this case we wait until all the map tasks are finished
			task = c.GetWaitTask()
		} else {
			c.MapTaskDone = true
			task = c.GetWaitTask()
		}
		c.mmu.Unlock()
		if !task.TaskStartTime.IsZero() {
			*reply = task
		}
	} else {
		c.rmu.Lock()
		c.DealReduceTaskTimeout()
		if c.ReduceTasksLen() > 0 {
			task = c.PopReduceTask()
			task.TaskStartTime = time.Now()
			c.PendingRedcueTasks[task.TaskId] = task
		} else if c.PendingRedcueTasksLen() > 0 {
			task = c.GetWaitTask()
		} else { // end the task
			c.ReduceTaskDone = true
			task = c.GetEndTask()
		}
		c.rmu.Unlock()
		if !task.TaskStartTime.IsZero() {
			*reply = task
		}
	}

	return nil
}


func (c *Coordinator) TaskDone(resp *Response, otioseReply *int) error {

	if resp.TaskType == DMapTask {
		c.mmu.Lock()
		delete(c.PendingMapTasks, resp.TaskId)
		c.mmu.Unlock()
	} else if resp.TaskType == DReduceTask {
		c.rmu.Lock()
		delete(c.PendingRedcueTasks, resp.TaskId)
		c.rmu.Unlock()
	} else {
		log.Fatalln("panic: unknown task type")
	}
	return nil

}


func (c *Coordinator) PopMapTask() TaskInfo {
	task := c.MapTasks[0]
	c.MapTasks = append(c.MapTasks[:0], c.MapTasks[1:]...)
	return task
}


func (c *Coordinator) PopReduceTask() TaskInfo {
	task := c.ReduceTasks[0]
	c.ReduceTasks= append(c.ReduceTasks[:0], c.ReduceTasks[1:]...)
	return task
}


func (c *Coordinator) MapTaskLen() int {
	return len(c.MapTasks)
}


func (c *Coordinator) PendingMapTasksLen() int {
	return len(c.PendingMapTasks)
}


func (c *Coordinator) ReduceTasksLen() int {
	return len(c.ReduceTasks)
}


func (c *Coordinator) PendingRedcueTasksLen() int {
	return len(c.PendingRedcueTasks)
}


func (c *Coordinator) DealMapTaskTimeout() {

	t := time.Now()

	for _, val := range c.PendingMapTasks {
		dur := t.Sub(val.TaskStartTime)
		if dur > c.MapTaskTimeoutThreshold {
			// this task time out
			val.BackUpTask = true
			c.MapTasks = append(c.MapTasks, val)
		}
	}

}


func (c *Coordinator) DealReduceTaskTimeout() {

	t := time.Now()

	for _, val := range c.PendingRedcueTasks {
		dur := t.Sub(val.TaskStartTime)
		if dur > c.ReduceTaskTimeoutThreshold {
			// this task time out
			val.BackUpTask = true
			c.ReduceTasks = append(c.ReduceTasks, val)
		}
	}

}


func (c *Coordinator) GetWaitTask() TaskInfo {
	task := TaskInfo{
		DWaitTask, 0, 0, time.Now(), "", false,
	}
	return task
}


func (c *Coordinator) GetEndTask() TaskInfo {
	task := TaskInfo {
		DEndTask, 0, 0, time.Now(), "", false,
	}
	return task
}
