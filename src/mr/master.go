package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Task struct {
	TaskNum int
	File    string
	State   string
}

type Master struct {
	// Your definitions here.
	mu       sync.Mutex
	TaskList []Task
	NReduce  int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Job(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range TaskList {
		if task.State == "Idle" {
			reply.task = task
			reply.NReduce = m.NReduce
			return nil
		}
	}
}

func (m *Master) State(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range TaskList {
		if task.TaskNum == args.TaskNum {
			task.State = args.State
			task.File = args.File
			return nil
		}
	}
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
	ret := true

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.TaskList {
		if task.State != "Finish" {
			ret = false
			break
		}
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}
	TaskList := []Task{}
	for i, file := range files {
		task := Task{i, file, "Idle"}
		TaskList[i] = task
	}

	m.TaskList = TaskList
	m.NReduce = nReduce

	// Your code here.

	m.server()
	return &m
}
