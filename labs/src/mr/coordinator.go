package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type mapTask struct {
	id        int
	inputfile string
	startTime int64
	status    string
}

type reduceTask struct {
	id        int
	startTime int64
	status    string
}

type Coordinator struct {
	mu            sync.Mutex
	phase         string // map, reduce or done
	mapTasks      []mapTask
	numMapDone    int
	reduceTasks   []reduceTask
	numReduceDone int
	timeout       int64
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().Unix()
	switch c.phase {
	case "done":
		reply.TaskType = Done
		return nil
	case "map":
		for i, task := range c.mapTasks {
			stale := task.status == "running" && (now-task.startTime) > c.timeout
			if task.status == "pending" || stale {
				log.Printf("Assgin map task %d (stale=%v)", task.id, stale)
				c.mapTasks[i].status = "running"
				c.mapTasks[i].startTime = now
				reply.TaskType = Map
				reply.TaskID = i
				reply.NumReduce = len(c.reduceTasks)
				reply.InputFile = task.inputfile
				return nil
			}
		}
	case "reduce":
		for i, task := range c.reduceTasks {
			stale := task.status == "running" && (now-task.startTime) > c.timeout
			if task.status == "pending" || stale {
				log.Printf("Assgin reduce task %d (stale=%v)", task.id, stale)
				c.reduceTasks[i].status = "running"
				c.reduceTasks[i].startTime = now
				reply.TaskType = Reduce
				reply.TaskID = i
				reply.NumMap = len(c.mapTasks)
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case Map:
		if c.mapTasks[args.TaskID].status != "done" {
			c.mapTasks[args.TaskID].status = "done"
			c.numMapDone += 1
			log.Printf("Map task %d done\n", args.TaskID)
		}
		if c.numMapDone == len(c.mapTasks) {
			c.phase = "reduce"
			log.Printf("Starting reduce phase with %d reducer tasks\n", len(c.reduceTasks))
		}
	case Reduce:
		if c.reduceTasks[args.TaskID].status != "done" {
			c.reduceTasks[args.TaskID].status = "done"
			c.numReduceDone += 1
			log.Printf("Reduce task %d done\n", args.TaskID)
		}
		if c.numReduceDone == len(c.reduceTasks) {
			c.phase = "done"
			log.Printf("Done\n")
		}
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == "done"
}

func removeFiles(pattern string) {
	files, err := filepath.Glob(pattern)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	println("Initializing coordinator")
	// Make sure to remove intermediate files from previous runs
	removeFiles("tmp-mr-out-*")
	c.phase = "map"
	c.numMapDone = 0
	c.mapTasks = make([]mapTask, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = mapTask{i, files[i], 0, "pending"}
	}
	c.numReduceDone = 0
	c.reduceTasks = make([]reduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = reduceTask{i, 0, "pending"}
	}
	c.timeout = 10
	c.server()
	return &c
}
