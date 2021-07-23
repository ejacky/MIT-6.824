package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStat int

const FINISHED TaskStat = 1
const INIT TaskStat = 0

type ReduceTask struct {
	filename string
	state    TaskStat
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nMap    int
	nReduce int

	ReduceTask map[string]TaskStat
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetMapTask(args *Args, reply *TaskMapReply) error {
	if len(c.files) == 0 {
		reply.Done = true
	} else {
		reply.MapNumber = c.nMap - len(c.files)
		reply.Filename = c.files[0]
		reply.NReduce = c.nReduce

		c.files = c.files[1:]
	}

	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishedMapArgs, reply *ExampleReply) error {
	for _, filename := range args.Filenames {
		c.ReduceTask[filename] = INIT
	}
	return nil
}

func (c *Coordinator) GetReduceTask(args *Args, reply *TaskReduceReply) error {
	for filename, stat := range c.ReduceTask {
		if stat == INIT {
			reply.Filename = filename
			break
		}
	}

	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishedReduceArgs, reply *FinishedReduceReply) error {
	c.ReduceTask[args.Filename] = FINISHED
	reply.State = FINISHED
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
	}
	// Your code here.

	c.server()
	return &c
}
