package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskInfo struct {
	TaskType int
	TaskId int
	NReduce int

	TaskStartTime time.Time
	Fname string
	BackUpTask bool
}

type Response struct {  // send this struct to coordinator when finishing task
	TaskType int
	TaskId int
}

const (
	DMapTask int = 1
	DReduceTask = 2
	DWaitTask = 3
	DEndTask = 4
)


type MapTaskResultInfo struct {

}


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
