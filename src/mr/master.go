package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	State      string
	TaskNum    int
	File       string
	IsSent     bool
	IsFinished bool
}

type Reply struct {
	Task    Task
	NReduce int
	NFiles  int
}

type Args struct {
	State   string
	TaskNum int
}

type Master struct {
	// Your definitions here.

	mu      sync.Mutex
	NReduce int
	NFiles  int

	MapTask    []Task
	ReduceTask []Task

	MapCount    int
	ReduceCount int

	IsFinished bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskJob(args *Args, reply *Reply) error {
	// why nReduce always zero in worker
	reply.Task = Task{"No job", 0, "nil", false, false}
	reply.NReduce = m.NReduce
	reply.NFiles = m.NFiles

	if !CheckIfMapFinish(m) {
		SendMap(m, reply)
		go CheckIfTaskTimeOut(m, reply)
	} else if !CheckIfReduceFinish(m) {
		SendReduce(m, reply)
		go CheckIfTaskTimeOut(m, reply)
	} else {
		m.IsFinished = true
		t := Task{"Finish", 0, "nil", true, true}
		reply.Task = t
	}
	return nil
}

func CheckIfMapFinish(m *Master) bool {
	if m.MapCount >= 8 {
		return true
	}
	return false
}

func CheckIfReduceFinish(m *Master) bool {
	if m.ReduceCount >= 10 {
		return true
	}
	return false
}

func CheckIfTaskTimeOut(m *Master, reply *Reply) {

	<-time.After(10 * time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()
	t := reply.Task
	if t.State == "Map" {
		if !m.MapTask[t.TaskNum].IsFinished {
			m.MapTask[t.TaskNum].IsSent = false
			fmt.Println("the Map %v is time out", t.TaskNum)
		}
	} else if t.State == "Reduce" {
		if !m.ReduceTask[t.TaskNum].IsFinished {
			m.ReduceTask[t.TaskNum].IsSent = false
			fmt.Println("the Reduce %v is time out", t.TaskNum)

		}
	}
}

func SendMap(m *Master, reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, _ := range m.MapTask {
		if !m.MapTask[i].IsSent {
			m.MapTask[i].IsSent = true
			reply.Task = m.MapTask[i]
			break
		}
	}
}

func SendReduce(m *Master, reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, _ := range m.ReduceTask {
		if !m.ReduceTask[i].IsSent && !m.ReduceTask[i].IsFinished {
			m.ReduceTask[i].IsSent = true
			reply.Task = m.ReduceTask[i]
			break
		}
	}
}

func (m *Master) ReportJob(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := args.State
	i := args.TaskNum

	if state == "Map" && !m.MapTask[i].IsFinished {
		m.MapTask[i].IsFinished = true
		if m.MapCount == -1 {
			m.MapCount = 0
		}
		m.MapCount++
		fmt.Println("finish map %v job", i)
		fmt.Println("current map job is %v", m.MapCount)
	} else if state == "Reduce" && !m.ReduceTask[i].IsFinished {
		m.ReduceTask[i].IsFinished = true
		if m.ReduceCount == -1 {
			m.ReduceCount = 0
		}
		m.ReduceCount++
		if m.ReduceCount >= len(m.ReduceTask) {

			m.IsFinished = true
		}
		fmt.Println("finish reduce %v job", i)
		fmt.Println("current reduce job is %v", m.ReduceCount)
	}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := false
	// Your code here.
	ret = m.IsFinished
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.
	m := Master{}
	m.NReduce = nReduce
	m.NFiles = len(files)
	m.MapCount = 0
	m.ReduceCount = 0
	m.IsFinished = false

	m.MapTask = make([]Task, len(files))
	for i, _ := range files {
		t := Task{"Map", i, files[i], false, false}
		m.MapTask[i] = t
	}

	for i := 0; i < nReduce; i++ {
		task := Task{"Reduce", i, "nil", false, false}
		m.ReduceTask = append(m.ReduceTask, task)
	}
	m.server()
	return &m
}
