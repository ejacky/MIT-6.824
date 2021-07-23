package mr

import (
	"encoding/json"
	"sort"
	"strings"

	//"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// map task
	// uncomment to send the Example RPC to the coordinator.
	intermediate := []KeyValue{}
	for {
		task := CallGetMapTask()
		if task.Done {
			break
		}
		filename := task.Filename
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
			iname := fmt.Sprintf("mr-%d-%d", task.MapNumber, ihash(kv.Key))

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
		CallFinishMapTask(getMapKeys(encoderMap))
	}

	for {
		reduceTask := CallGetReduceTask()
		if reduceTask.Done {
			break
		}
		rfile, _ := os.Create(reduceTask.Filename)
		dec := json.NewDecoder(rfile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		sort.Sort(ByKey(intermediate))

		t := strings.Split(reduceTask.Filename, "-")
		oname := "mr-out-" + t[2]
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
		CallFinishReduceTask(reduceTask.Filename)
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
	fmt.Printf("reply.Filename %v\n", reply.Filename)
	return reply
}

func CallFinishMapTask(filenames []string) TaskMapReply {
	args := FinishedMapArgs{
		Filenames: filenames,
	}
	// declare a reply structure.
	reply := TaskMapReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.FinishMapTask", &args, &reply)
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
	fmt.Printf("reply.Filename %v\n", reply.Filename)
	return reply
}

func CallFinishReduceTask(filename string) TaskReduceReply {
	args := FinishedReduceArgs{
		Filename: filename,
	}
	// declare a reply structure.
	reply := TaskReduceReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.FinishReduceTask", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

func getMapKeys(m map[string]*json.Encoder) []string {
	var keys = make([]string, 0, len(m))
	for key, _ := range m {
		keys = append(keys, key)
	}

	return keys
}

func closeCreateFile(m map[string]*os.File) {
	for _, file := range m {
		file.Close()
	}
}
