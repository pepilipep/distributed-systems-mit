package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	workerID := uuid.New().String()

	for {
		reply, ok := CallAskForTask(workerID)
		if !ok {
			break
		}

		if reply.OK && reply.TaskResponse != nil {
			switch reply.TaskResponse.TaskType {
			case MAP:
				intermediate := []KeyValue{}
				for _, filename := range reply.TaskResponse.FileNames {
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
					intermediate = append(intermediate, kva...)
				}

				encs := make([]*json.Encoder, reply.TaskResponse.NReduce)
				for rNumber := 0; rNumber < reply.TaskResponse.NReduce; rNumber++ {
					filename := fmt.Sprintf("mr-%v-%v", reply.TaskResponse.Number, rNumber)
					file, err := os.Create(filename)
					if err != nil {
						log.Fatalf("cannot create %v", filename)
					}
					encs[rNumber] = json.NewEncoder(file)
				}

				for _, kv := range intermediate {
					err := encs[ihash(kv.Key)%reply.TaskResponse.NReduce].Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write to %v", encs[ihash(kv.Key)%reply.TaskResponse.NReduce])
					}
				}

			case REDUCE:

				kva := []KeyValue{}
				for _, filename := range reply.TaskResponse.FileNames {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				sort.Sort(ByKey(kva))

				oname := fmt.Sprintf("mr-out-%v", reply.TaskResponse.Number)
				ofile, _ := os.Create(oname)

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
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}
			default:
				fmt.Println("wtf is this shit??")
			}
		}

		time.Sleep(time.Second)
	}

}

func CallAskForTask(workerID string) (AskForTaskReply, bool) {
	args := AskForTaskArgs{WorkerID: workerID}

	reply := AskForTaskReply{}

	ok := call("Master.AskForTask", &args, &reply)

	return reply, ok
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
