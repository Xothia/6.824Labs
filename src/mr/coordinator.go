package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//需求
//1.任务超过10秒未完成则重新分配
//2.当所有map reduce任务完成时要在done返回true
//3.worker断线需要检测
//5.worker通过rpc请求一个任务(map或者reduce)
//6.并发锁！

type AFile struct {
	IsDispatched bool //indicate weather a filename is dispatched
	IsDone       bool
	WorkerId     int
}
type AWorker struct {
	Filename string
	IsAlive  bool //indicate weather a worker is alive
}

type Coordinator struct {
	// Your definitions here.
	//remember to lock data
	Files            map[string]*AFile //Filename -> AFile
	FilesLock        sync.Mutex
	Workers          map[int]*AWorker //worker id -> worker
	WorkersLock      sync.Mutex
	nReduce          int  //number of reduce tasks to use.
	allMapTaskIsDone bool //
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *Reply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// RegisterWorker workers call this func to register
func (c *Coordinator) RegisterWorker(workerId int, reply *Reply) error {
	reply.Status = 200

	c.WorkersLock.Lock()

	w := new(AWorker)
	w.IsAlive = true
	c.Workers[workerId] = w

	c.WorkersLock.Unlock()

	return nil
}

func (c *Coordinator) ReqTask(workerId int, reply *Reply) error {
	//default 200
	reply.Status = 200
	for filename, afile := range c.Files {
		if !afile.IsDispatched {
			//assign map task
			reply.Status = 201
			reply.Data = filename
		}
	}
	return nil
}

// ImAlive alive detect
func (c *Coordinator) ImAlive(workerId int, reply *Reply) error {
	c.Workers[workerId].IsAlive = true
	reply.Status = 200
	//log.Printf("Worker:%d is still alive", workerId)
	return nil
}

func aliveValidation(seconds int, c *Coordinator) error {
	for {
		//sleep given seconds
		time.Sleep(time.Duration(seconds) * time.Second)

		//check workers weather is alive
		for id, worker := range c.Workers {
			if !worker.IsAlive {
				log.Printf("worker:%d is dead. redispatching %s task", id, worker.Filename)
				//solution
				//可能是map任务失败或者是reduce任务失败

			}
			worker.IsAlive = false
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Println("Coordinator begin to serve. Socket name:" + coordinatorSock())
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	done := true
	for _, afile := range c.Files {
		if !afile.IsDone {
			done = false
		}
	}

	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// My code Begin.
	//new a Coordinator
	c := new(Coordinator)
	c.Workers = make(map[int]*AWorker, 10)
	c.Files = make(map[string]*AFile, 10)
	c.nReduce = nReduce

	//init Files
	filenames := os.Args[1:]
	for _, name := range filenames {
		c.Files[name] = new(AFile)
	}

	go aliveValidation(1, c)
	// My code end.
	c.server()
	return c
}
