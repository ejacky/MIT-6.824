package mr

import (
	"MIT-6.824/src/util"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"

	//"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// map task
	// uncomment to send the Example RPC to the coordinator.

	var wg sync.WaitGroup
	wg.Add(2)

	// map phase
	go handleMap(mapf, &wg)

	// reduce phase
	go handleReduce(reducef, &wg)

	wg.Wait()
}

func handleMap(mapf func(string, string) []KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		task := CallGetMapTask()
		if task.Stat != GET || task.TaskId == "" {
			time.Sleep(time.Second * 5)
			continue
		}
		filename := task.TaskId
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		encoderMap := make(map[string]*json.Encoder)
		fileMap := make(map[string]*os.File)
		for _, kv := range kva {
			iname := fmt.Sprintf("mr-%d-%d", util.Ihash(task.TaskId), util.Ihash(kv.Key)%task.NReduce)

			if _, ok := encoderMap[iname]; !ok {
				// path/to/whatever does not exist
				ifile, _ := os.Create(iname)
				enc := json.NewEncoder(ifile)
				encoderMap[iname] = enc
				fileMap[iname] = ifile
			}

			_ = encoderMap[iname].Encode(&kv)
		}

		closeCreateFile(fileMap)

		// commit finished
		CallFinishMapTask(strconv.Itoa(util.Ihash(task.TaskId)))
	}
}

func handleReduce(reducef func(string, []string) string, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		reduceTask := CallGetReduceTask()
		if reduceTask.Stat != GET || reduceTask.TaskId == "" {
			time.Sleep(time.Second * 5)
			continue
		}

		intermediate := []KeyValue{}
		for _, mapId := range reduceTask.MapIds {
			rfileName := fmt.Sprintf("mr-%s-%s", mapId, reduceTask.TaskId)
			rfile, _ := os.Open(rfileName)
			dec := json.NewDecoder(rfile)

			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			rfile.Close()
		}

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + reduceTask.TaskId
		ofile, _ := os.Create(oname)
		//fmt.Println(intermediate)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
		CallFinishReduceTask(reduceTask.TaskId)
	}
}

func CallGetMapTask() TaskMapReply {
	// declare an argument structure.
	args := Args{}

	// declare a reply structure.
	reply := TaskMapReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GetMapTask", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("GetMapTask %v\n", reply)
	return reply
}

func CallFinishMapTask(taskId string) TaskMapReply {
	args := FinishedMapArgs{
		TaskId: taskId,
	}
	// declare a reply structure.
	reply := TaskMapReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.FinishMapTask", &args, &reply)
	fmt.Printf("FinishMapTask %v\n", taskId)

	return reply
}

func CallGetReduceTask() TaskReduceReply {
	// declare an argument structure.
	args := Args{}

	// declare a reply structure.
	reply := TaskReduceReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GetReduceTask", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("GetReduceTask %v\n", reply)
	return reply
}

func CallFinishReduceTask(taskId string) TaskReduceReply {
	args := FinishedReduceArgs{
		TaskId: taskId,
	}
	// declare a reply structure.
	reply := TaskReduceReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.FinishReduceTask", &args, &reply)

	fmt.Printf("FinishReduceTask %v\n", taskId)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func closeCreateFile(m map[string]*os.File) {
	for _, file := range m {
		file.Close()
	}
}
