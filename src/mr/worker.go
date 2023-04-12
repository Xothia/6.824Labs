package mr

import (
	"fmt"
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
		task := askForTask(workerId)
		processTask(task.Status, task.Data)

		time.Sleep(11 * time.Second)
	}

}

func processTask(taskStatus int, filename string) {

	switch taskStatus {
	case 201:
		//Map Task
		log.Println("processing map task.filename:" + filename)
	case 202:
		//Reduce Task
		log.Println("processing reduce task.filename:" + filename)
	case 203:
		//No un-dispatched tasks
		log.Println("No un-dispatched tasks.")

	case 204: //all work done
		log.Println("all work done.")
	}

}

func askForTask(workerId int) *Reply {
	reply := new(Reply)
	ok := call("Coordinator.ReqTask", workerId, reply)
	if ok {
		log.Println("Worker get a task:" + reply.Data)
	} else {
		log.Println("Get task failed!")
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
