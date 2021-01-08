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
	State   string
	TaskNum int
	File    string
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

	if !m.CheckIfMapFinish() {
		m.SendMap(reply)
		go m.CheckIfTaskTimeOut(reply)
	} else if !m.CheckIfReduceFinish() {
		m.SendReduce(reply)
		go m.CheckIfTaskTimeOut(reply)
	} else {
		m.IsFinished = true
		reply.State = "Finish"
	}
	return nil
}

func (m *Master) CheckIfMapFinish() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.MapCount >= 8 {
		return true
	}
	return false
}

func (m *Master) CheckIfReduceFinish() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ReduceCount >= 10 {
		return true
	}
	return false
}

func (m *Master) CheckIfTaskTimeOut(reply *Reply) {

	<-time.After(10 * time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()
	State := reply.State
	TaskNum := reply.TaskNum

	if State == "Map" {
		if !m.MapTask[TaskNum].IsFinished {
			m.MapTask[TaskNum].IsSent = false
			fmt.Printf("map job %v is timeout\n", TaskNum)
		}
	} else if State == "Reduce" {
		if !m.ReduceTask[TaskNum].IsFinished {
			m.ReduceTask[TaskNum].IsSent = false

			fmt.Printf("reduce job %v is timeout\n", TaskNum)
		}
	}
}

func (m *Master) SendMap(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.State = "No job"
	reply.TaskNum = 0
	reply.File = "nil"
	reply.NReduce = m.NReduce
	reply.NFiles = m.NFiles

	for i, _ := range m.MapTask {
		if !m.MapTask[i].IsSent {
			m.MapTask[i].IsSent = true
			reply.State = "Map"
			reply.TaskNum = i
			reply.File = m.MapTask[i].File
			fmt.Printf("Send map job %v \n", reply.TaskNum)
			break
		}
	}
}

func (m *Master) SendReduce(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.State = "No job"
	reply.TaskNum = 0
	reply.File = "nil"
	reply.NReduce = m.NReduce
	reply.NFiles = m.NFiles

	for i, _ := range m.ReduceTask {
		if !m.ReduceTask[i].IsSent {
			m.ReduceTask[i].IsSent = true
			reply.State = "Reduce"
			reply.TaskNum = i
			fmt.Printf("Send reduce job %v \n", reply.TaskNum)
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
		m.MapCount++
		fmt.Printf("map job %v is finished\n", i)
	} else if state == "Reduce" && !m.ReduceTask[i].IsFinished {
		m.ReduceTask[i].IsFinished = true
		m.ReduceCount++
		if m.ReduceCount >= len(m.ReduceTask) {

			m.IsFinished = true
		}

		fmt.Printf("reduce job %v is finished\n", i)
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
		m.MapTask[i] = Task{"Map", i, files[i], false, false}
	}

	m.ReduceTask = make([]Task, nReduce)
	for i, _ := range m.ReduceTask {
		m.ReduceTask[i] = Task{"Reduce", i, "nil", false, false}
	}
	m.server()
	return &m
}
