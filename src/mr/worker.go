package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	sockname := coordinatorSock()
	// client, err := rpc.DialHTTP("tcp", ":1234")
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		err = client.Call("Coordinator.GetTask", &args, &reply)
		if err != nil {
			log.Fatal("Call Coordinator.GetTask:", err)
		}
		// fmt.Println("Coordinator.GetTask:", reply.TaskType)
		switch reply.TaskType {
		case MapTask:
			inputFile, err := os.Open(reply.FilePath)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FilePath)
			}
			content, err := ioutil.ReadAll(inputFile)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FilePath)
			}
			inputFile.Close()
			kva := mapf(reply.FilePath, string(content))

			// every map task will output NReduce files
			outputFiles := make([]*os.File, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)

			for i := 0; i < reply.NReduce; i++ {
				// reply.TaskId is map TaskID and i is reduce TaskID
				// filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				// file, _ := os.Create(filename)
				tempFile, _ := os.CreateTemp(".", "mr-mapTemp-*")
				outputFiles[i] = tempFile
				encoders[i] = json.NewEncoder(tempFile)
			}
			for _, kv := range kva {
				reduceID := ihash(kv.Key) % reply.NReduce
				err = encoders[reduceID].Encode(&kv)
			}

			if err != nil {
				log.Fatalf("encoding error: %v", err)
			}

			for i := 0; i < reply.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				err = outputFiles[i].Close() // must close before rename
				if err != nil {
					log.Fatalf("cannot close %v", filename)
				}
				err = os.Rename(outputFiles[i].Name(), filename)
				if err != nil {
					os.Remove(outputFiles[i].Name())
					log.Fatalf("cannot rename %v", filename)
				}
			}

			reportArgs := ReportTaskArgs{reply.TaskType, reply.TaskId}
			reportReply := ReportTaskReply{}
			err = client.Call("Coordinator.ReportTask", &reportArgs, &reportReply)
			if err != nil {
				log.Fatalf("Call Coordinator.ReportTask: %v", err)
			}
			break
		case ReduceTask:
			var kva []KeyValue
			for i := 0; i < reply.NMap; i++ {
				// i is map TaskID and reply.TaskId is reduce TaskID
				filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
			// ofile, _ := os.Create(oname)

			tempFile, _ := os.CreateTemp(".", "mr-reduceTemp-*")

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			tempFile.Close()
			os.Rename(tempFile.Name(), oname)

			reportArgs := ReportTaskArgs{reply.TaskType, reply.TaskId}
			reportReply := ReportTaskReply{}
			err = client.Call("Coordinator.ReportTask", &reportArgs, &reportReply)
			if err != nil {
				log.Fatalf("Call Coordinator.ReportTask: %v", err)
			}
			break
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			os.Exit(0)
		default:
			panic("unhandled default case")
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
