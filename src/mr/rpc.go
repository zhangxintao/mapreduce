package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	Map    = 1
	Reduce = 2
)

type AskTaskReqeust struct {
}

type MapTaskResponse struct {
	TaskNumber int
	FilePath   string
	NReduce    int
}
type ReduceTaskResponse struct {
	TaskNumber        int
	IntermediateFiles []string
}
type AskTaskResponse struct {
	TaskType   int
	MapTask    MapTaskResponse
	ReduceTask ReduceTaskResponse
}

type CompleteTaskRequest struct {
	TaskType int
	Map      CompleteMapTaskRequest
	Reduce   CompleteReduceTaskRequest
}

type CompleteMapTaskRequest struct {
	MapTaskNum  int
	OutputFiles map[int]string
}

type CompleteTaskResponse struct {
}

type CompleteReduceTaskRequest struct {
	ReduceTastNum int
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
