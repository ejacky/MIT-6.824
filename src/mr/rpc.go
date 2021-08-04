package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Args struct {
	X int
}

type Reply struct {
	Y int
}

type TaskMapReply struct {
	Done    bool
	NReduce int
	TaskId  string
}

type FinishedMapArgs struct {
	TaskId string
}

type FinishedMapReply struct {
	State TaskStat
}

type TaskReduceReply struct {
	Done    bool
	NReduce int
	TaskId  string
	MapIds  []string
}

type FinishedReduceArgs struct {
	TaskId string
}

type FinishedReduceReply struct {
	State TaskStat
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
