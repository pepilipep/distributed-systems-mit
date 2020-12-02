package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskState int8

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	State    TaskState
	WorkerID *string
}

type Master struct {
	// Your definitions here.
	Tasks   map[Task]bool
	Files   []string
	NReduce int
}

var times = 0
var interFiles = []string{}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	if times == 0 {
		reply.OK = true
		reply.TaskResponse = &TaskReponse{
			TaskType:  MAP,
			Number:    0,
			NReduce:   m.NReduce,
			FileNames: m.Files,
		}
	} else if times <= m.NReduce {
		reply.OK = true
		reply.TaskResponse = &TaskReponse{
			TaskType:  REDUCE,
			Number:    times - 1,
			NReduce:   m.NReduce,
			FileNames: interFiles,
		}
	} else {
		reply.OK = false
	}

	times++

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	return times > m.NReduce
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.Files = files
	m.NReduce = nReduce

	for i := 0; i < nReduce; i++ {
		interFiles = append(interFiles, fmt.Sprintf("mr-0-%v", i))
	}

	// Your code here.

	m.server()
	return &m
}
