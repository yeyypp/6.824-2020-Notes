package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var sb strings.Builder

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// RPC parameters

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
	fmt.Println("worker is working")
	args := Args{}
	reply := Reply{}

	for {
		ok := call("Master.Job", &args, &reply)
		if !ok {
			fmt.Println("It's over")
			return
		}
		fmt.Println("asking for job")
		switch reply.CurTask.State {
		case "Map":
			fmt.Println("Doing map job")
			doMap(mapf, &reply)
		case "Reduce":
			fmt.Println("Doing reduce job")
			doReduce(reducef, &reply)
		case "Working":
			fmt.Println("Still working")
			time.Sleep(time.Duration(time.Second * 10))
		case "Finish":
			fmt.Println("Tasks completed")
			return

		}
	}

	// uncomment to send the Example RPC to the master.
	//	CallExample()

}

func GetFileName(TaskNum, HashNum int) string {
	return fmt.Sprintf("mr-%d-%d", TaskNum, HashNum)
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

	TempFiles := make([]*os.File, reply.NReduce)
	for i := 0; i < len(TempFiles); i++ {
		TempFiles[i], _ = ioutil.TempFile("", "mr-tmp-*")
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		f := TempFiles[index]
		enc := json.NewEncoder(f)
		if e := enc.Encode(&kv); e != nil {
			fmt.Println("File %v Key %v Value %v Error %v", f.Name(), kv.Key, kv.Value, e)
			panic("Json encode failed")
		}
	}

	for i, f := range TempFiles {
		newName := GetFileName(task.TaskNum, i)
		oldName := filepath.Join(f.Name())
		os.Rename(oldName, newName)
		f.Close()
	}

	args := Args{"Map Done", task.TaskNum}
	re := Reply{}
	call("Master.State", &args, &re)
}

func doReduce(reducef func(string, []string) string, reply *Reply) {
	index := reply.CurTask.TaskNum
	NFiles := reply.NFiles
	for count := 0; count < NFiles; count++ {
		fileName := "mr-" + strconv.Itoa(count) + "-" + strconv.Itoa(index)
		kva := []KeyValue{}
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println("can't open the %v", fileName)
			return

		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()

		sort.Sort(ByKey(kva))
		ofile, e := ioutil.TempFile("", "mr-*")
		if e != nil {
			fmt.Println("create tem file error")
		}
		oname := "mr-out-" + strconv.Itoa(index)
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

		os.Rename(filepath.Join(ofile.Name()), oname)
		ofile.Close()
	}

	args := Args{"Reduce Done", 0}
	re := Reply{}
	call("Master.State", &args, &re)
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
