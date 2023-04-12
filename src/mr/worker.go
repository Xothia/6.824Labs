package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KVSlice []KeyValue

func (K KVSlice) Len() int {
	return len(K)
}
func (K KVSlice) Less(i, j int) bool {
	return K[i].Key < K[j].Key
}
func (K KVSlice) Swap(i, j int) {
	K[i], K[j] = K[j], K[i]
}

// use ihash(key) % NReduce to choose reduce task number
// for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//workerId := 782932
	workerId := time.Now().Nanosecond()
	heartbeatPeriod := 300 // Millisecond
	log.Printf("worker:%d begin to work.\n", workerId)
	log.Println("socket name:" + coordinatorSock())

	// Your worker implementation here.
	registerToCoordinator(workerId)       //register present worker
	go imAlive(workerId, heartbeatPeriod) //sending alive signal periodically

	for { //main loop
		reply := askForTask(workerId)
		outputFilename := processTask(reply, mapf, reducef) //processing

		switch reply.Status {
		case 201: //map task done
			callMapTaskDone(TaskDoneReqArgs{
				WorkerId:       workerId,
				OutputFilename: outputFilename,
			})
		case 202: //reduce task done

		case 204: //all work done.
			break
		}

	}

}

// return OutputFilename
func processTask(reply *AskForTaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) string {
	//intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
	outputFilename := ""
	status := reply.Status
	filename := reply.Filename
	nReduce := reply.NReduce

	switch status {
	case 201:
		//The map phase should divide the intermediate keys into buckets for nReduce
		//reduce tasks, where nReduce is the number of reduce tasks --
		//the argument that main/mrcoordinator.go passes to MakeCoordinator().
		//Each mapper should create nReduce intermediate files for consumption
		//by the reduce tasks.
		//filepath.Glob
		log.Println("processing map task.filename:" + filename)

		//read file
		var intermediate []KeyValue
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		//init a buket
		buket := make([]KVSlice, nReduce)
		for i := 0; i < nReduce; i++ {
			kvSlice := make(KVSlice, 50)
			buket[i] = kvSlice
		}
		for _, kv := range intermediate { //push kvs into buket
			i := ihash(kv.Key)
			buket[i] = append(buket[i], kv)
		}
		for i := 0; i < nReduce; i++ { //sort and write file
			sort.Sort(buket[i]) //sort
			//write file
			outputFilename = "mr-" + filename + "-" + strconv.Itoa(i)
			midFile, _ := os.Open(outputFilename)
			enc := json.NewEncoder(midFile)
			for _, kv := range buket[i] {
				err := enc.Encode(&kv)
				log.Fatal(err.Error())
			}
			//	TODO rename after complete write
		}
		outputFilename = "mr-" + filename + "-*"

	case 202:
		//Reduce Task
		// put the output of the X'th reduce task in the file mr-out-X.
		log.Println("processing reduce task.filename:" + filename)
		outputFilename = filename
	case 203:
		//No un-dispatched tasks
		log.Println("No un-dispatched tasks.")

	case 204: //all work done
		log.Println("all work done.")
	}
	time.Sleep(7 * time.Second)
	return outputFilename
}

func callMapTaskDone(args TaskDoneReqArgs) *Reply {
	reply := new(Reply)
	ok := call("Coordinator.MapTaskDone", args, reply)
	if !ok {
		log.Println("callMapTaskDone failed!")
	} else {
		//log.Println("Worker get a task:" + reply.Data)
	}
	return reply
}

func askForTask(workerId int) *AskForTaskReply {
	reply := new(AskForTaskReply)
	ok := call("Coordinator.ReqTask", workerId, reply)
	if !ok {
		log.Println("Get task failed!")
	} else {
		//log.Println("Worker get a task:" + reply.Data)
	}
	return reply
}

func registerToCoordinator(workerId int) {
	reply := new(Reply)
	ok := call("Coordinator.RegisterWorker", workerId, reply)
	if ok {
		log.Println("Worker Has Registered.")
	} else {
		log.Println("Register failed!")
	}
}

func imAlive(workerId int, period int) {
	for {
		ok := call("Coordinator.ImAlive", workerId, nil)
		if !ok {
			log.Println("Alive signal sending failed!")
		} else {
			//log.Printf("Worker:%d Alive signal has sent.", workerId)
		}
		time.Sleep(time.Duration(period) * time.Millisecond)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample(workerId int) {

	reply := new(Reply)

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.ImAlive", workerId, reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("worker recive reply ImAlive:%d\n", reply.Status)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
