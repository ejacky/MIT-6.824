package mr

import (
	"MIT-6.824/src/util"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStat int

const WAIT TaskStat = 2
const FINISHED TaskStat = 1
const GET TaskStat = 0

const Timeout = 50

type Coordinator struct {
	// Your definitions here.
	mapTask    *TaskMap
	reduceTask *TaskReduce
}

type Info struct {
	Start time.Time
	End   time.Time
	File  string
}

type TaskMap struct {
	nMap  int
	Queue []string
	Task  map[string]*Info
	state TaskStat
	mu    sync.Mutex
}

func (m *TaskMap) init(files []string) {
	m.Task = make(map[string]*Info, len(files))
	for _, file := range files {
		taskId := strconv.Itoa(util.Ihash(file))
		m.Task[taskId] = &Info{
			File: file,
		}

		m.Queue = append(m.Queue, taskId)
	}
}

func (m *TaskMap) check() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for taskId, task := range m.Task {
		if task.Start.IsZero() {
			continue
		}

		if !task.End.IsZero() {
			continue
		}

		// 超过 Timeout 秒没有上报完成， 重新入队
		if time.Now().Sub(task.Start).Seconds() > Timeout {
			fmt.Printf("存在超时 %v 秒的 map 任务,taskId=%v, task= %v\n", Timeout, taskId, task)
			m.Queue = append(m.Queue, taskId)
			m.Task[taskId].Start = time.Now()
			m.state = GET
		}
	}
}

func (m *TaskMap) get() string {
	if len(m.Queue) == 0 {
		return ""
	}
	taskId := m.Queue[0]
	m.Queue = m.Queue[1:]

	m.Task[taskId].Start = time.Now()

	return taskId
}

func (m *TaskMap) getFileName() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskId := m.get()

	if taskId == "" {
		return ""
	}

	return m.Task[taskId].File
}

func (m *TaskMap) done(taskId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Task[taskId].End = time.Now()

	if len(m.Queue) == 0 && m.allDone() {
		m.state = FINISHED
	} else if len(m.Queue) == 0 {
		m.state = WAIT
	}
}

func (m *TaskMap) allDone() bool {

	for _, task := range m.Task {
		if task.End.IsZero() {
			return false
		}
	}
	return true
}

func (m *TaskMap) getState() TaskStat {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state
}

type TaskReduce struct {
	NReduce int
	Queue   []string
	Task    map[string]*Info
	state   TaskStat
	mu      sync.Mutex
}

func (r *TaskReduce) init(nReduce int) {
	r.Task = make(map[string]*Info, nReduce)
	for i := 0; i < nReduce; i++ {
		taskId := strconv.Itoa(i)
		r.Queue = append(r.Queue, taskId)
		r.Task[taskId] = &Info{}
	}
	r.NReduce = nReduce

}

func (r *TaskReduce) check() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for taskId, task := range r.Task {
		if task.Start.IsZero() {
			continue
		}

		if !task.End.IsZero() {
			continue
		}

		//
		//fmt.Printf("check reduce 任务,taskId=%v, task= %v\n", taskId, task)

		// 开始超过 90 秒，没有上报完成， 重新入队
		if time.Now().Sub(task.Start).Seconds() > Timeout {
			fmt.Printf("存在超时的 reduce 任务,taskId=%v, task= %v\n", taskId, task)
			r.Queue = append(r.Queue, taskId)
			r.Task[taskId].Start = time.Now()
			r.state = GET
		}
	}
}

func (r *TaskReduce) get() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Queue) == 0 {
		return ""
	}

	taskId := r.Queue[0]
	r.Queue = r.Queue[1:]

	r.Task[taskId].Start = time.Now()

	return taskId
}

func (r *TaskReduce) done(taskId string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Task[taskId].End = time.Now()

	if len(r.Queue) == 0 && r.allDone() {
		r.state = FINISHED
	} else if len(r.Queue) == 0 {
		r.state = WAIT
	}
}

func (r *TaskReduce) allDone() bool {

	for _, task := range r.Task {
		if task.End.IsZero() {
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
	if c.mapTask.getState() == FINISHED {
		reply.Stat = FINISHED
	} else if c.mapTask.getState() == WAIT {
		reply.Stat = WAIT
	} else {
		reply.Stat = GET
		reply.NReduce = c.reduceTask.NReduce
		reply.TaskId = c.mapTask.getFileName()
	}

	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishedMapArgs, reply *Reply) error {

	c.mapTask.done(args.TaskId)

	return nil
}

func (c *Coordinator) GetReduceTask(args *Args, reply *TaskReduceReply) error {
	if c.reduceTask.getState() == FINISHED {
		reply.Stat = FINISHED
	} else if c.reduceTask.getState() == WAIT || c.mapTask.getState() != FINISHED {
		reply.Stat = WAIT
	} else if c.mapTask.getState() == FINISHED {
		reply.Stat = GET
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

func getMapKeys(m map[string]*Info) []string {
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

	// Your code here.
	if c.reduceTask.getState() == FINISHED {
		return true
	}

	if c.mapTask.getState() != FINISHED {
		c.mapTask.check()
	} else {
		c.reduceTask.check()
	}

	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTask:    &TaskMap{},
		reduceTask: &TaskReduce{},
	}
	c.init(files, nReduce)

	c.server()
	return &c
}
