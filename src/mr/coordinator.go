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

// MapTask Map Task input files
type MapTask struct {
	IsDispatched bool //indicate weather a filename is dispatched
	IsDone       bool //do not need this
	WorkerId     int
	lock         sync.Mutex
}

// ReduceTask Reduce Task input files
type ReduceTask struct {
	IsDispatched bool //indicate weather a filename is dispatched
	IsDone       bool
	WorkerId     int
	lock         sync.Mutex
}

// AWorker all workers
type AWorker struct {
	Filename      string
	TaskBeginTime time.Time
	IsAlive       bool //indicate weather a worker is alive
}

type Coordinator struct {
	// Your definitions here.
	//remember to lock data
	//map tasks
	MapTasks     map[string]*MapTask //Filename -> MapTask
	MapTasksLock sync.RWMutex
	//reduce tasks
	ReduceTasks     map[string]*ReduceTask //Filename -> ReduceTask
	ReduceTasksLock sync.RWMutex

	Workers     map[int]*AWorker //worker id -> worker
	WorkersLock sync.RWMutex

	nReduce          int  //number of reduce tasks to use.
	allMapTaskIsDone bool //
	aliveDetectionT  int  //seconds
	tleDetectionT    int  //seconds
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

	w := new(AWorker)
	w.IsAlive = true

	c.WorkersLock.Lock()
	c.Workers[workerId] = w
	c.WorkersLock.Unlock()

	return nil
}

func (c *Coordinator) ReqTask(workerId int, reply *Reply) error {
	//default -1
	reply.Status = -1

	c.MapTasksLock.RLock()
	c.ReduceTasksLock.RLock()
	if len(c.MapTasks) != 0 { //map tasks not done yet
		c.ReduceTasksLock.RUnlock()
		reply.Status = 203

		for filename, mapTask := range c.MapTasks {
			mapTask.lock.Lock()
			if !mapTask.IsDispatched {
				c.MapTasksLock.RUnlock()
				//assign map task
				reply.Status = 201
				reply.Data = filename
				mapTask.IsDispatched = true
				mapTask.lock.Unlock()
				break
			}
			mapTask.lock.Unlock()
		}
		c.MapTasksLock.RUnlock()

	} else if len(c.ReduceTasks) != 0 { //reduce tasks not done yet
		c.MapTasksLock.RUnlock()
		reply.Status = 203
		for filename, reduceTask := range c.ReduceTasks {
			reduceTask.lock.Lock()
			if !reduceTask.IsDispatched {
				c.ReduceTasksLock.RUnlock()
				//assign reduce task
				reply.Status = 202
				reply.Data = filename
				reduceTask.IsDispatched = true
				reduceTask.lock.Unlock()
				break
			}
			reduceTask.lock.Unlock()
		}
		c.ReduceTasksLock.RUnlock()

	} else { //all work done
		reply.Status = 204
		c.MapTasksLock.RUnlock()
		c.ReduceTasksLock.RUnlock()
	}

	return nil
}

func (c *Coordinator) MapTaskDone(arg TaskDoneReqArgs, reply *Reply) error {

	return nil
}

func (c *Coordinator) ReduceTaskDone(arg TaskDoneReqArgs, reply *Reply) error {

	return nil
}

// ImAlive alive detect
func (c *Coordinator) ImAlive(workerId int, reply *Reply) error {
	//update do not need to write lock
	c.WorkersLock.RLock()
	c.Workers[workerId].IsAlive = true
	c.WorkersLock.RUnlock()

	reply.Status = 200
	//log.Printf("Worker:%d is still alive", workerId)
	return nil
}

// TLEDetection periodically check AWorker list
func TLEDetection(c *Coordinator) error {
	for {
		time.Sleep(time.Duration(c.tleDetectionT) * time.Second)
	}
	return nil
}

func aliveDetection(c *Coordinator) error {
	for {
		//sleep given seconds
		time.Sleep(time.Duration(c.aliveDetectionT) * time.Second)

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
	for _, afile := range c.MapTasks {
		if !afile.IsDone {
			done = false
			break
		}
	}

	for _, bfile := range c.ReduceTasks {
		if !bfile.IsDone {
			done = false
			break
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
	c.MapTasks = make(map[string]*MapTask, 10)
	c.ReduceTasks = make(map[string]*ReduceTask, 10)
	c.aliveDetectionT = 1
	c.tleDetectionT = 1
	c.nReduce = nReduce

	//init Files
	filenames := os.Args[1:]
	for _, name := range filenames {
		c.MapTasks[name] = new(MapTask)
	}

	go aliveDetection(c)
	// My code end.
	c.server()
	return c
}
