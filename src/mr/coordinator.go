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
	WorkerId     int
	lock         sync.Mutex
}

// ReduceTask Reduce Task input files
type ReduceTask struct {
	IsDispatched bool //indicate weather a filename is dispatched
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
	tleLimit         int  //seconds
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
				//c.MapTasksLock.RUnlock()
				//assign map task
				reply.Status = 201
				reply.Data = filename

				mapTask.IsDispatched = true
				mapTask.WorkerId = workerId
				mapTask.lock.Unlock()

				c.WorkersLock.RLock()
				aWorker := c.Workers[workerId]
				c.WorkersLock.RUnlock()
				aWorker.TaskBeginTime = time.Now()
				aWorker.Filename = filename
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
				//c.ReduceTasksLock.RUnlock()
				//assign reduce task
				reply.Status = 202
				reply.Data = filename

				reduceTask.IsDispatched = true
				reduceTask.WorkerId = workerId
				reduceTask.lock.Unlock()

				c.WorkersLock.RLock()
				aWorker := c.Workers[workerId]
				c.WorkersLock.RUnlock()
				aWorker.TaskBeginTime = time.Now()
				aWorker.Filename = filename
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

// MapTaskDone delete a map task and add a reduce task
func (c *Coordinator) MapTaskDone(arg TaskDoneReqArgs, reply *Reply) error {
	//what if a tle worker call this function?
	reply.Status = 200
	c.WorkersLock.RLock()
	if worker, ok := c.Workers[arg.WorkerId]; ok && len(worker.Filename) != 0 {
		filename := worker.Filename
		c.deleteMapTask(filename)
		c.ReduceTasksLock.Lock()
		r := new(ReduceTask)
		c.ReduceTasks[arg.OutputFilename] = r
		c.ReduceTasksLock.Unlock()
	} else {
		reply.Status = 301 //caller is a dead or tle worker
	}
	c.WorkersLock.RUnlock()
	return nil
}

func (c *Coordinator) ReduceTaskDone(arg TaskDoneReqArgs, reply *Reply) error {

	return nil
}

// ImAlive alive detect
func (c *Coordinator) ImAlive(workerId int, reply *Reply) error {
	//update do not need to write lock
	c.WorkersLock.RLock()
	if _, ok := c.Workers[workerId]; ok {
		c.Workers[workerId].IsAlive = true
	}
	c.WorkersLock.RUnlock()

	reply.Status = 200
	//log.Printf("Worker:%d is still alive", workerId)
	return nil
}

// TLEDetection periodically check AWorker list
func TLEDetection(c *Coordinator) error {
	for {
		time.Sleep(time.Duration(c.tleDetectionT) * time.Second)
		c.WorkersLock.RLock()
		for workerId, aWorker := range c.Workers {
			limit := c.tleLimit
			seconds := time.Now().Sub(aWorker.TaskBeginTime).Seconds()
			if len(aWorker.Filename) != 0 && aWorker.IsAlive && seconds > float64(limit) {
				//tle
				log.Printf("worker:%d tle.redispatching %s task", workerId, aWorker.Filename)
				retrievingTask(c, aWorker)
			}
		}
		c.WorkersLock.RUnlock()
	}
}

func aliveDetection(c *Coordinator) error {

	for {
		//sleep given seconds
		time.Sleep(time.Duration(c.aliveDetectionT) * time.Second)

		//check workers weather is alive
		c.WorkersLock.RLock()
		for id, worker := range c.Workers {

			if !worker.IsAlive {
				//solution
				//可能是map任务失败或者是reduce任务失败
				if len(worker.Filename) != 0 {
					log.Printf("worker:%d is dead. redispatching %s task", id, worker.Filename)
					retrievingTask(c, worker)
				} else {
					log.Printf("worker:%d is dead. ", id)
				}
			}
			//maybe concurrency issue ImAlive
			worker.IsAlive = false
		}
		c.WorkersLock.RUnlock()
	}
}

//when a worker disabled something need to be done
func retrievingTask(c *Coordinator, worker *AWorker) { //worker disabled

	if mapTask, ok1 := c.MapTasks[worker.Filename]; ok1 {
		mapTask.lock.Lock()
		mapTask.IsDispatched = false
		mapTask.WorkerId = -1
		mapTask.lock.Unlock()
	} else if reduceTask, ok2 := c.ReduceTasks[worker.Filename]; ok2 {
		reduceTask.lock.Lock()
		reduceTask.IsDispatched = false
		reduceTask.WorkerId = -1
		reduceTask.lock.Unlock()
	}
	worker.TaskBeginTime = time.Time{}
	worker.Filename = ""

}

func (c *Coordinator) deleteWorker(workerId int) {
	c.WorkersLock.Lock()
	delete(c.Workers, workerId)
	c.WorkersLock.Unlock()
}
func (c *Coordinator) deleteMapTask(filename string) {
	c.MapTasksLock.Lock()
	delete(c.MapTasks, filename)
	c.MapTasksLock.Unlock()
}
func (c *Coordinator) deleteReduceTask(filename string) {
	c.ReduceTasksLock.Lock()
	delete(c.ReduceTasks, filename)
	c.ReduceTasksLock.Unlock()
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	done := false
	c.MapTasksLock.RLock()
	c.ReduceTasksLock.RLock()
	if len(c.MapTasks) == 0 && len(c.ReduceTasks) == 0 {
		done = true
	}
	c.MapTasksLock.RUnlock()
	c.ReduceTasksLock.RUnlock()

	return done
}

// MakeCoordinator create a Coordinator.
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
	c.tleLimit = 10
	c.nReduce = nReduce

	//init Files
	filenames := os.Args[1:]
	for _, name := range filenames {
		c.MapTasks[name] = new(MapTask)
	}

	//submissions
	go aliveDetection(c)
	go TLEDetection(c)
	// My code end.
	c.server()
	return c
}
