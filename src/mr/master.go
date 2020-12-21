package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

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
	State   string
	CurTask Task
	NReduce int
}

type Args struct {
	State    string
	TaskNum  int
	TaskList []Task
}

type Master struct {
	// Your definitions here.
	mu      sync.Mutex
	NReduce int
	Phase   string

	MapTask   []Task
	MapCount  int
	MapFinish int

	ReduceTask   []Task
	ReduceCount  int
	ReduceFinish int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Job(args *Args, reply *Reply) error {
	switch m.Phase {
	case "Start":
		DistributeMapJob(m, reply)
	case "Mapping":
		Process(m, reply)
	case "Reduce":
		DistributeReduceJob(m, reply)
	case "Reducing":
		Process(m, reply)
	case "Finish":
		Finish(m, reply)
	}
	return nil
}

func DistributeMapJob(m *Master, reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, t := range m.MapTask {
		if t.State == "idle" {
			m.MapTask[i].State = "map"
			reply.State = "Map"
			reply.CurTask = m.MapTask[i]
			reply.NReduce = m.NReduce
			m.MapCount += 1
			if m.MapCount == len(m.MapTask) {
				m.Phase = "Mapping"
			}
			break
		}
	}

}

func Process(m *Master, reply *Reply) {
	switch m.Phase {
	case "Mapping":
		reply.State = "Mapping"
	case "Reducing":
		reply.State = "Reducing"
	}
}

func DistributeReduceJob(m *Master, reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, t := range m.ReduceTask {
		if t.State == "reduce" {
			m.ReduceTask[i].State = "reducing"
			reply.State = "Reduce"
			reply.CurTask = m.ReduceTask[i]
			reply.NReduce = m.NReduce
			m.ReduceCount += 1
			if m.ReduceCount == len(m.ReduceTask) {
				m.Phase = "Reducing"
			}
			break
		}
	}

}

func Finish(m *Master, reply *Reply) {
	reply.State = "Finish"
}

func (m *Master) State(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.State {
	case "Map Done":
		for i, _ := range args.TaskList {
			m.ReduceTask = append(m.ReduceTask, args.TaskList[i])
		}
		//m.ReduceTask = append(m.ReduceTask, args.TaskList...)
		m.MapFinish += 1
		if m.MapFinish == len(m.MapTask) {
			m.Phase = "Reduce"
		}
	case "Reduce Done":
		m.ReduceFinish += 1
		if m.ReduceFinish == m.AllJob {
			m.Phase = "Finish"
		}
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
	m := Master{}
	m.Phase = "Start"
	m.NReduce = nReduce
	m.MapTask = make([]Task, len(files))
	for i, f := range files {
		m.MapTask[i] = Task{"idle", i, f}
	}
	m.MapCount = 0
	m.MapFinish = 0

	m.ReduceTask = make([]Task, 0)
	m.ReduceCount = 0
	m.ReduceFinish = 0

	m.AllJob = len(files) * nReduce

	m.server()
	return &m
}
