package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

/*
	Status
	Common
	-1		error
	200		ok

		Coordinator.ReqTask
		201		Map Task
		202		Reduce Task
		203		No un-dispatched tasks
		204		all work done

		Coordinator.MapTaskDone
		301		caller is a dead or tle worker
*/

type TaskDoneReqArgs struct {
	WorkerId       int
	OutputFilename string
}

type Reply struct {
	Status int
	Data   string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
