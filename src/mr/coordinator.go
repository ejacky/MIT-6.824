package mr

import (
	"MIT-6.824/src/util"
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStat int

const WAIT TaskStat = 2
const FINISHED TaskStat = 1
const GET TaskStat = 0

type Coordinator struct {
	// Your definitions here.
	mapTask    TaskMap
	reduceTask TaskReduce
}

type TaskMap struct {
	nMap  int
	Queue []string
	Task  map[string]TaskStat
	state TaskStat
	mu    sync.Mutex
}

func (m *TaskMap) init(files []string) {
	m.Queue = append(m.Queue, files...)
	m.Task = make(map[string]TaskStat, len(files))
}

func (m *TaskMap) get() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskId := m.Queue[0]
	m.Queue = m.Queue[1:]

	m.Task[strconv.Itoa(util.Ihash(taskId))] = GET

	return taskId
}

func (m *TaskMap) done(taskId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Task[taskId] = FINISHED

	if len(m.Queue) == 0 && m.allDone() {
		m.state = FINISHED
	} else if len(m.Queue) == 0 {
		m.state = WAIT
	}
}

func (m *TaskMap) allDone() bool {

	for _, task := range m.Task {
		if task != FINISHED {
			return false
		}
	}
	return true
}

type TaskReduce struct {
	NReduce int
	Queue   []string
	Task    map[string]TaskStat
	state   TaskStat
	mu      sync.Mutex
}

func (r *TaskReduce) init(nReduce int) {
	for i := 0; i < nReduce; i++ {
		r.Queue = append(r.Queue, strconv.Itoa(i))
	}
	r.NReduce = nReduce
	r.Task = make(map[string]TaskStat, nReduce)
}

func (r *TaskReduce) get() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Queue) == 0 {
		return ""
	}

	taskId := r.Queue[0]
	r.Queue = r.Queue[1:]

	r.Task[taskId] = GET

	return taskId
}

func (r *TaskReduce) done(taskId string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Task[taskId] = FINISHED

	if len(r.Queue) == 0 && r.allDone() {
		r.state = FINISHED
	} else if len(r.Queue) == 0 {
		r.state = WAIT
	}
}

func (r *TaskReduce) allDone() bool {

	for _, task := range r.Task {
		if task != FINISHED {
			return false
		}
	}
	return true
}

func (r *TaskReduce) getState() TaskStat {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) init(files []string, nReduce int) {
	c.mapTask.init(files)
	c.reduceTask.init(nReduce)
}

func (c *Coordinator) GetMapTask(args *Args, reply *TaskMapReply) error {
	if c.mapTask.state == FINISHED || c.mapTask.state == WAIT {
		reply.Done = true
	} else {
		reply.NReduce = c.reduceTask.NReduce
		reply.TaskId = c.mapTask.get()
	}

	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishedMapArgs, reply *Reply) error {

	c.mapTask.done(args.TaskId)

	return nil
}

func (c *Coordinator) GetReduceTask(args *Args, reply *TaskReduceReply) error {
	if c.reduceTask.state == FINISHED || c.reduceTask.state == WAIT || c.mapTask.state != FINISHED {
		reply.Done = true
	} else {
		reply.NReduce = c.reduceTask.NReduce
		reply.TaskId = c.reduceTask.get()
		reply.MapIds = getMapKeys(c.mapTask.Task)
	}

	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishedReduceArgs, reply *FinishedReduceReply) error {
	c.reduceTask.done(args.TaskId)

	return nil
}

func getMapKeys(m map[string]TaskStat) []string {
	var keys = make([]string, 0, len(m))
	for key, _ := range m {
		keys = append(keys, key)
	}

	return keys
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
	if c.reduceTask.getState() == FINISHED {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.init(files, nReduce)

	c.server()
	return &c
}
