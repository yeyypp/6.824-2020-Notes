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

type Task struct {
	State   string
	TaskNum int
	File    string
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
	reply.NReduce = m.NReduce
	reply.NFiles = m.NFiles

	if !CheckIfMapFinish(m) {
		SendMap(m, reply)
	} else if !CheckIfReduceFinish(m) {
		SendReduce(m, reply)
	} else {
		t := Task{"Finish", 0, "nil"}
		reply.Task = t
		return nil
	}
}

func CheckIfMapFinish(m *Master) bool {
	if m.MapCount >= len(m.MapTask) {
		return true
	}
	return false
}

func CheckIfReduceFinish(m *Master) bool {
	if m.ReduceCount >= len(m.ReduceTask) {
		return true
	}
	return false
}

func CheckIfTaskTimeOut(t Task) {
}

func SendMap(m *Master, reply *Reply) {
}

func SendReduce(m *Master, reply *Reply) {

}

func (m *Master) ReportJob(args *Args, reply *Reply) error {

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
	ret := false
	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.

	m.server()
	return &m
}
