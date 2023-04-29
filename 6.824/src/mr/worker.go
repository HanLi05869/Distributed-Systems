package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapAndReduceWorker struct {
	Id      int
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

type TaskPhase int

const (
	MapPhase     TaskPhase = 0
	ReducePhase  TaskPhase = 1
	WaitingPhase TaskPhase = 1
)

type Task struct {
	FileName   string
	TaskNumber int
	Phase      TaskPhase
	NReduce    int
	NMap       int
	Alive      bool
}

type RequestWorker struct {
	Id int
}

type ResponseTaskReply struct {
	State int
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	
	w := MapAndReduceWorker{}
	w.Mapf = mapf
	w.Reducef = reducef
	
	w.register()
	
	w.run()
}

func (w *MapAndReduceWorker) register() {
	reply := RequestWorker{}
	call("Coordinator.Register", &RequestWorker{}, &reply)
	w.Id = reply.Id
}

func (w *MapAndReduceWorker) run() {
	for {
		t := w.requestTask()
		if !t.Alive {
			fmt.Printf("not Alive, quit\n")
			return
		}
		if t.TaskNumber == -1 {
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}

		w.doTask(t)
	}

}

func (w *MapAndReduceWorker) requestTask() Task {
	args := RequestWorker{}
	args.Id = w.Id
	t := Task{}
	call("Coordinator.RequestTask", &args, &t)
	return t
}

func (w *MapAndReduceWorker) doTask(t Task) {
	if t.Phase == MapPhase {
		w.doMapTask(t)
	} else if t.Phase == ReducePhase {
		w.doReduceTask(t)
	} else {
		fmt.Printf("do task error/n")
	}
}

func (w *MapAndReduceWorker) doMapTask(t Task) {
	// read files
	file, err := os.Open(t.FileName)
	if err != nil {
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}
	file.Close()

	// 运行mapf
	kva := w.Mapf(t.FileName, string(content))

	// the reslut of map is stored into slice 
	reduce := make([][]KeyValue, t.NReduce)
	for index := range reduce {
		reduce[index] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		reduce[ihash(kv.Key)%t.NReduce] = append(reduce[ihash(kv.Key)%t.NReduce], kv)
	}

	// slice transfered to output file, the name format is %v %v
	for index, ys := range reduce {
		if len(ys) > 0 {
			reduceFileName := "mr-" + strconv.Itoa(t.TaskNumber) + "-" + strconv.Itoa(index)
			ofile, _ := os.Create(reduceFileName)
			for _, y := range ys {
				fmt.Fprintf(ofile, "%v %v\n", y.Key, y.Value)
			}
			ofile.Close()
		}
	}

	w.responseTask(t)
}

func (w *MapAndReduceWorker) doReduceTask(t Task) {
	intermediate := []KeyValue{}

	for i := 0; i <= t.NMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(t.TaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			//log.Printf("cannot open %v", filename)
			continue
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			//log.Printf("cannot read %v", filename)
			continue
		}
		file.Close()
		// content is converted to KeyValue
		// split contents into an array of words.
		lines := strings.Split(string(content), "\n")
		
		for _, w := range lines {
			kvArr := strings.Split(w, " ")
			if len(kvArr) <= 1 {
				continue
			}
			kv := KeyValue{kvArr[0], kvArr[1]}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(t.TaskNumber)
	ofile, _ := os.Create(oname)
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
		output := w.Reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	w.responseTask(t)
}

func (w *MapAndReduceWorker) responseTask(t Task) {
	reply := ResponseTaskReply{}
	call("Coordinator.ResponseTask", t, &reply)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
	log.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
