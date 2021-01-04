package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
func (m *Master) Job(args *Args, reply *Reply) error {
	// why nReduce always zero in worker
	reply.NReduce = m.NReduce
	reply.NFiles = m.NFiles

}

func SendMap(m *Master, reply *Reply) {
	t := m.MapQ.Poll()
	reply.CurTask = t
	m.RunningQ.Offer(t)

}

func SendReduce(m *Master, reply *Reply) {
	t := m.ReduceQ.Poll()
	reply.CurTask = t
	m.RunningQ.Offer(t)
}

func (m *Master) State(args *Args, reply *Reply) error {

	switch args.State {
	case "Map Done":
		m.RunningQ.Poll()
	case "Reduce Done":
		m.RunningQ.Poll()
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
	ret := true

	// Your code here.
	if m.Phase != "Finish" {
		ret = false
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.
	m := Master{NReduce: nReduce, NFiles: len(files)}
	m.Phase = "Start"
	m.MapQ = NewQueue()
	m.ReduceQ = NewQueue()
	m.RunningQ = NewQueue()

	for i, _ := range files {
		t := Task{"Map", i, files[i]}
		m.MapQ.Offer(t)
	}

	for i := 0; i < nReduce; i++ {
		t := Task{"Reduce", i, ""}
		m.ReduceQ.Offer(t)
	}

	m.server()
	return &m
}
