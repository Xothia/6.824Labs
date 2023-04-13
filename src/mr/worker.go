package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	//log.Printf("worker:%d begin to work.\n", workerId)
	//log.Println("socket name:" + coordinatorSock())

	// Your worker implementation here.
	registerToCoordinator(workerId)       //register present worker
	go imAlive(workerId, heartbeatPeriod) //sending alive signal periodically

	for { //main loop
		reply := askForTask(workerId)
		outputFilename := processTask(reply, mapf, reducef) //processing

		switch reply.Status {
		case 201: //map task done
			callMapTaskDone(TaskDoneReqArgs{workerId, outputFilename})
		case 202: //reduce task done
			callReduceTaskDone(TaskDoneReqArgs{workerId, outputFilename})
		case 204: //all work done.
			//break
		case 301: //caller is a dead or tle worker
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
		//log.Println("processing map task.filename:" + filename)

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
		_ = file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		//init a buket
		buket := make([]KVSlice, nReduce)
		for i := 0; i < nReduce; i++ {
			kvSlice := make(KVSlice, 0)
			buket[i] = kvSlice
		}
		for _, kv := range intermediate { //push kvs into buket
			i := ihash(kv.Key) % nReduce
			buket[i] = append(buket[i], kv)
		}
		randFilename := strconv.Itoa(time.Now().Nanosecond())
		for i := 0; i < nReduce; i++ { //sort and write file
			sort.Sort(buket[i]) //sort FIRST
			//buket[i] = Reduce(buket[i], reducef) //THEN call reduce on one buket
			//write file

			outputFilename = "mr-" + randFilename + "-" + strconv.Itoa(i)
			writeMidFile(buket[i], outputFilename)
		}
		outputFilename = "mr-" + randFilename + "-*"

	case 202:
		//Reduce Task: put the output of the X'th reduce task in the file mr-out-X.
		taskFilenames, err := filepath.Glob(filename)
		if err != nil {
			log.Fatal("filepath.Glob(filename) went wrong.")
		}
		//log.Println("processing reduce task.filename:" + filename)
		//read file
		midKVS := make(KVSlice, 0)
		for _, filename := range taskFilenames {
			//read file
			amidFile, _ := os.Open(filename)
			dec := json.NewDecoder(amidFile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				midKVS = append(midKVS, kv)
			}
			_ = amidFile.Close()
		}
		sort.Sort(midKVS)                //sort first!
		midKVS = Reduce(midKVS, reducef) //call reduce func
		//build output filename
		runes := []rune(filename)
		lastChar := string(runes[len(runes)-1])
		opFilename := "mr-out-" + lastChar
		writeOutFile(midKVS, opFilename)

		outputFilename = opFilename
	case 203:
		//No un-dispatched tasks
		//log.Println("No un-dispatched tasks.")

	case 204: //all work done
		//log.Println("all work done.")
	}
	//time.Sleep(3 * time.Second)
	return outputFilename
}
func writeOutFile(content KVSlice, outputFilename string) {
	//write file with %v %v format
	tempFilename := strconv.Itoa(time.Now().Nanosecond())
	midFile, _ := os.Create(tempFilename)
	for _, kv := range content {
		_, err := fmt.Fprintf(midFile, "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatal("write file wrong:" + err.Error())
		}
	}
	_ = midFile.Close()
	err := os.Rename(tempFilename, outputFilename)
	if err != nil {
		log.Fatalln("writeOutFile can not rename")
	}
}

func writeMidFile(content KVSlice, outputFilename string) {
	//write file
	tempFilename := strconv.Itoa(time.Now().Nanosecond())
	midFile, _ := os.Create(tempFilename)
	enc := json.NewEncoder(midFile)
	for _, kv := range content {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("write file wrong:" + err.Error())
		}
	}
	_ = midFile.Close()
	err := os.Rename(tempFilename, outputFilename)
	if err != nil {
		log.Fatalln("writeMidFile can not rename")
	}
}

func Reduce(kvs KVSlice, reducef func(string, []string) string) KVSlice {
	//reduce a SORTED KVSlice
	res := make(KVSlice, 0)
	// call Reduce on each distinct key in kvs,
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		kv := KeyValue{kvs[i].Key, output}
		res = append(res, kv)

		i = j
	}
	return res
}

func callReduceTaskDone(args TaskDoneReqArgs) *Reply {
	reply := new(Reply)
	ok := call("Coordinator.ReduceTaskDone", args, reply)
	if !ok {
		log.Println("callReduceTaskDone failed!")
	} else {
		//log.Println("Worker get a task:" + reply.Data)
	}
	return reply
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
		log.Fatalln("Get task failed!")
	} else {
		//log.Println("Worker get a task:" + reply.Data)
	}
	return reply
}

func registerToCoordinator(workerId int) {
	reply := new(Reply)
	ok := call("Coordinator.RegisterWorker", workerId, reply)
	if ok {
		//log.Println("Worker Has Registered.")
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
