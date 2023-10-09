package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func finishTask(taskType string, id int) {
	args := FinishTaskArgs{taskType, id}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		return
	}
	log.Printf("Coordinator.FinishTask failed - maybe coordinator is donw?")
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	done := false
	for !done {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			log.Println("Coordinator.GetTask failed")
			time.Sleep(1 * time.Second)
		}

		switch reply.TaskType {
		case "done":
			log.Printf("[%d] Coordinator says done. Stopping worker...", os.Getpid())
			done = true
		case "map":
			log.Printf("[%d] working on map %d %s\n", os.Getpid(), reply.TaskID, reply.InputFile)
			mapID := reply.TaskID
			content := readFile(reply.InputFile)
			intermediate := mapf(reply.InputFile, content)
			groupByReduceID := make(map[int][]KeyValue)
			for _, kv := range intermediate {
				reduceID := ihash(kv.Key) % reply.NumReduce
				groupByReduceID[reduceID] = append(groupByReduceID[reduceID], kv)
			}
			for reduceID := range groupByReduceID {
				reduceFilename := fmt.Sprintf("tmp-mr-out-%d-%d", mapID, reduceID)
				tmpfile, err := ioutil.TempFile("/tmp", reduceFilename)
				if err != nil {
					panic(err)
				}
				enc := json.NewEncoder(tmpfile)
				for _, kv := range groupByReduceID[reduceID] {
					err := enc.Encode(&kv)
					if err != nil {
						panic(err)
					}
					// fmt.Printf("%+v\n", kv)
				}
				tmpfile.Close()
				os.Rename(tmpfile.Name(), reduceFilename)
			}
		case "reduce":
			log.Printf("[%d] working on reduce %d %s\n", os.Getpid(), reply.TaskID, reply.InputFile)
			reduceID := reply.TaskID
			kva := []KeyValue{}
			for mapID := 0; mapID < reply.NumMap; mapID++ {
				filename := fmt.Sprintf("tmp-mr-out-%d-%d", mapID, reduceID)
				_, err := os.Stat(filename)
				if err != nil && os.IsNotExist(err) {
					continue
				}
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Cannot open %s", filename)
				}
				dec := json.NewDecoder(file)
				for {
					kv := KeyValue{}
					if err := dec.Decode(&kv); err != nil {
						break
					}
					// fmt.Printf("%+v\n", kv)
					kva = append(kva, kv)
				}
			}
			// log.Printf("[%d] Done with reading intermediate files\n", os.Getpid())
			grouped := make(map[string][]string)
			for _, kv := range kva {
				grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
			}
			outfile := fmt.Sprintf("mr-out-%d", reduceID)
			tmpfile, err := ioutil.TempFile("/tmp", outfile)
			if err != nil {
				panic(err)
			}
			// log.Printf("[%d] Running reduce function...\n", os.Getpid())
			for key := range grouped {
				res := reducef(key, grouped[key])
				// fmt.Printf("res=%s\n", res)
				_, err := tmpfile.WriteString(key + " " + res + "\n")
				if err != nil {
					panic(err)
				}
			}
			tmpfile.Close()
			// log.Printf("[%d] Renaming output file...\n", os.Getpid())
			os.Rename(tmpfile.Name(), outfile)
		default:
			time.Sleep(1 * time.Second)
		}
		finishTask(reply.TaskType, reply.TaskID)
	}
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

	return false
}
