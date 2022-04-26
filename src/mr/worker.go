package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"

)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


type ByKey []KeyValue

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
	for {
		task := CallDistributeTask()
		if task == nil {
			continue
		}
		switch task.TaskType {
		case DMapTask:
			ret := HandleMapTask(mapf, task)
			if ret {
				resp := Response{DMapTask, task.TaskId}
				CallTaskDone(&resp)
			}
		case DReduceTask:
			ret := HandleReduceTask(reducef, task)
			if ret {
				resp := Response{DReduceTask, task.TaskId}
				CallTaskDone(&resp)
			}
		case DWaitTask:
			continue
		case DEndTask:
			os.Exit(0)
		}
	}

}

func HandleMapTask(mapf func(string, string) []KeyValue, task *TaskInfo) bool {

	fname := task.Fname

	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("cannot open %v", fname)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fname)
	}
	file.Close()
	kva := mapf(fname, string(content))

	var files []*os.File
	var encs []*json.Encoder

	for i := 0; i < task.NReduce; i++ {

		f, _ := ioutil.TempFile("", "intermediate")
		files = append(files, f)
		encs = append(encs, json.NewEncoder(f))
	}

	for _, kv := range kva {
		encs[ihash(kv.Key)%10].Encode(&kv)
	}

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", task.TaskId, i)
		os.Rename(files[i].Name(), filename)
		files[i].Close()
	}
	return true
}

func HandleReduceTask(reducef func(string, []string) string, task *TaskInfo) bool {

	s := fmt.Sprintf("mr-\\d+-%v", task.TaskId)
	pwd, _ := os.Getwd()
	files, _ := ioutil.ReadDir(pwd)

	intermediate := []KeyValue{}
	for _, file := range files {
		if match, _ := regexp.MatchString(s, file.Name()); match {
			file, err := os.OpenFile(file.Name(), os.O_RDONLY, 0444)
			if err != nil {
				log.Fatalln("file open err: ", file.Name())
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
	}


	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return true
}


func CallTaskDone(resp *Response) bool {
	var otioseReply int
	ok := call("Coordinator.TaskDone", resp, &otioseReply)
	return ok
}


func CallDistributeTask() *TaskInfo {

	var otioseArgs int

	reply := TaskInfo{}
	ok := call("Coordinator.DistributeTask", &otioseArgs, &reply)

	if ok {
		return &reply
	}

	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
