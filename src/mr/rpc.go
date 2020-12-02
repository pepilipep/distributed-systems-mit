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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int8

const (
	MAP = iota
	REDUCE
)

type AskForTaskArgs struct {
	WorkerID string
}

type TaskReponse struct {
	TaskType  TaskType
	Number    int
	NReduce   int
	FileNames []string
}

type AskForTaskReply struct {
	OK           bool
	TaskResponse *TaskReponse
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
