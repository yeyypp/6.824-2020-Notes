package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Node struct {
	t    Task
	pre  *Node
	next *Node
}

type Queue struct {
	sync.RWMutex
	head *Node
	tail *Node
	size int
}

func NewQueue() *Queue {
	h := new(Node)
	t := new(Node)
	h.next = t
	t.pre = h
	return &Queue{
		head: h,
		tail: t,
		size: 0,
	}
}

func (q *Queue) Offer(t Task) {
	q.Lock()
	defer q.Unlock()

	n := new(Node)
	n.t = t

	pre := q.tail.pre
	pre.next = n
	n.pre = pre

	n.next = q.tail
	q.tail.pre = n
	q.size += 1
}

func (q *Queue) Poll() Task {
	q.Lock()
	defer q.Unlock()

	if q.size == 0 {
		return Task{}
	}

	n := q.head.next
	q.head.next = n.next
	n.next.pre = q.head

	n.next = nil
	n.pre = nil
	t := n.t

	q.size -= 1

	return t
}

func (q *Queue) Size() int {
	q.RLock()
	defer q.RUnlock()

	return q.size
}

func (q *Queue) IsEmpty() bool {
	q.RLock()
	defer q.RUnlock()

	return q.size == 0
}

type Task struct {
	State   string
	TaskNum int
	File    string
}

type Reply struct {
	CurTask Task
	NReduce int
	NFiles  int
}

type Args struct {
	State   string
	TaskNum int
}

type Master struct {
	// Your definitions here.
	NReduce int
	NFiles  int
	Phase   string

	MapQ     *Queue
	ReduceQ  *Queue
	RunningQ *Queue
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Job(args *Args, reply *Reply) error {
	// why nReduce always zero in worker
	reply.NReduce = m.NReduce
	reply.NFiles = m.NFiles

	if !m.MapQ.IsEmpty() {
		SendMap(m, reply)
	} else if m.MapQ.IsEmpty() && !m.RunningQ.IsEmpty() {
		t := Task{"Working", 0, "nil"}
		reply.CurTask = t
	} else if m.MapQ.IsEmpty() && !m.ReduceQ.IsEmpty() {
		SendReduce(m, reply)
	} else if m.ReduceQ.IsEmpty() && !m.RunningQ.IsEmpty() {
		t := Task{"Working", 0, "nil"}
		reply.CurTask = t
	} else if m.MapQ.IsEmpty() && m.ReduceQ.IsEmpty() && m.RunningQ.IsEmpty() {
		t := Task{"Finish", 0, "nil"}
		reply.CurTask = t
		m.Phase = "Finish"

	}
	return nil
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
