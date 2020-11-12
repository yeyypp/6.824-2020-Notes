package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Args struct {
	TaskNum int
	File    string
	State   string
}

type Reply struct {
	Phase   string
	CurTask Task
	NReduec int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := Args{State: "AskForJob"}
	reply := Reply{}

	hasJob := call("Master.Job", &args, &reply)
	if !hasJob {
		return
	}

	switch reply.Phase {
	case "Map":
		doMap(mapf, &reply)
	case "Reduce":
		doReduce(reducef, &reply)
	}
	// uncomment to send the Example RPC to the master.
	//	CallExample()

}

func doMap(mapf func(string, string) []KeyValue, reply *Reply) {
	task := reply.CurTask
	fileName := task.File
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	X := strconv.Itoa(task.TaskNum)
	for _, kv := range kva {
		tem := kv
		key := kv.Key
		Y := strconv.Itoa(ihash(key) % reply.NReduce)
		interFile := "mr" + X + "-" + Y
		ofile, _ := os.Create(interFile)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		err := enc.Encode(&tem)
		if err != nil {
			log.Fatalf("encode error")
		}
		args := Args{task.TaskNum, interFile, "ToBeReduced"}
		reply := Reply{}
		call("Master.State", &args, &reply)
	}
}

func doReduce(reducef func(string, []string) string, reply *Reply) {
	task := reply.CurTask
	file := task.File
	kva := []KeyValue{}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	sort.Sort(ByKey(kva))
	oname := "mr-out-" + task.TaskNum
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()

	args := Args{task.TaskNum, oname, "Finish"}
	reply := Reply{}
	call("Master.State", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
