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

type TaskState int8

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	State             TaskState
	Number            int
	WorkerID          string
	IntermediateFiles []string
	TimeStarted       time.Time
}

type Master struct {
	// Your definitions here.
	MapTasks       []Task
	ReduceTasks    []Task
	Files          []string
	NReduce        int
	MaxTimeWaiting time.Duration
}

var mapMux, reduceMux sync.Mutex

func (m *Master) handleCrashes() {
	mapMux.Lock()
	for i := range m.MapTasks {
		if m.MapTasks[i].State == InProgress {
			if m.MapTasks[i].TimeStarted.Add(m.MaxTimeWaiting).Before(time.Now().UTC()) {
				m.MapTasks[i] = Task{
					State:  Idle,
					Number: m.MapTasks[i].Number,
				}
			}
		}
	}
	mapMux.Unlock()

	reduceMux.Lock()
	for i := range m.ReduceTasks {
		if m.ReduceTasks[i].State == InProgress {
			if m.ReduceTasks[i].TimeStarted.Add(m.MaxTimeWaiting).Before(time.Now().UTC()) {
				m.ReduceTasks[i] = Task{
					State:  Idle,
					Number: m.ReduceTasks[i].Number,
				}
			}
		}
	}
	reduceMux.Unlock()
}

func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {

	m.handleCrashes()

	allMapsDone := true
	mapMux.Lock()
	for i, task := range m.MapTasks {
		if task.State != Idle {
			if task.State != Completed {
				allMapsDone = false
			}
			continue
		}
		m.MapTasks[i] = Task{
			State:       InProgress,
			Number:      m.MapTasks[i].Number,
			WorkerID:    args.WorkerID,
			TimeStarted: time.Now().UTC(),
		}
		reply.OK = true
		reply.TaskResponse = &TaskReponse{
			TaskType:  MAP,
			Number:    m.MapTasks[i].Number,
			NReduce:   m.NReduce,
			FileNames: []string{m.Files[m.MapTasks[i].Number]},
		}
		mapMux.Unlock()
		return nil
	}
	mapMux.Unlock()

	if !allMapsDone {
		reply.OK = false
		reply.TaskResponse = nil
		return nil
	}

	reduceMux.Lock()
	for i, task := range m.ReduceTasks {
		if task.State != Idle {
			continue
		}
		m.ReduceTasks[i] = Task{
			State:       InProgress,
			Number:      m.ReduceTasks[i].Number,
			WorkerID:    args.WorkerID,
			TimeStarted: time.Now().UTC(),
		}
		reply.OK = true

		var files []string

		mapMux.Lock()
		for _, f := range m.MapTasks {
			files = append(files, f.IntermediateFiles[m.ReduceTasks[i].Number])
		}
		mapMux.Unlock()

		reply.TaskResponse = &TaskReponse{
			TaskType:  REDUCE,
			Number:    m.ReduceTasks[i].Number,
			NReduce:   m.NReduce,
			FileNames: files,
		}
		reduceMux.Unlock()
		return nil
	}
	reduceMux.Unlock()
	reply.OK = false
	reply.TaskResponse = nil

	return nil
}

func (m *Master) DoneWithTask(args *DoneWithTaskArgs, reply *DoneWithTaskReply) error {

	switch args.TaskType {
	case MAP:
		mapMux.Lock()
		for i, t := range m.MapTasks {
			if t.Number != args.Number {
				continue
			}
			if t.WorkerID == args.WorkerID && t.State == InProgress {
				m.MapTasks[i].State = Completed
				m.MapTasks[i].IntermediateFiles = args.FileNames
				reply.OK = true
			} else {
				reply.OK = false
			}
			break
		}
		mapMux.Unlock()
		return nil
	case REDUCE:
		reduceMux.Lock()
		for i, t := range m.ReduceTasks {
			if t.Number != args.Number {
				continue
			}
			if t.WorkerID == args.WorkerID && t.State == InProgress {
				m.ReduceTasks[i].State = Completed
				reply.OK = true
			} else {
				reply.OK = false
			}
			break
		}
		reduceMux.Unlock()
		return nil
	default:
		reply.OK = false
	}

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
	reduceMux.Lock()
	for _, t := range m.ReduceTasks {
		if t.State != Completed {
			reduceMux.Unlock()
			return false
		}
	}
	reduceMux.Unlock()
	return true
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
	m.MapTasks = make([]Task, len(files))
	m.ReduceTasks = make([]Task, m.NReduce)
	m.MaxTimeWaiting = time.Second * 10

	for i := range m.MapTasks {
		m.MapTasks[i] = Task{
			State:  Idle,
			Number: i,
		}
	}

	for i := range m.ReduceTasks {
		m.ReduceTasks[i] = Task{
			State:  Idle,
			Number: i,
		}
	}

	m.server()
	return &m
}
