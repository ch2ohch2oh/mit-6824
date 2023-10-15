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

// Private type to track task status
type taskStatus string

const (
	UNASSIGNED taskStatus = "unassigned"
	ASSIGNED   taskStatus = "assigned"
	DONE       taskStatus = "done"
)

type mrTask struct {
	startTime int64
	status    taskStatus
}

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
	mu sync.Mutex

	mapFiles   []string
	mapTasks   []mrTask
	numMapDone int

	reduceTasks   []mrTask
	numReduceDone int

	// Assigned tasks running more than timeout will be reassigned to other workers
	timeout int64
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()

	if c.numMapDone < len(c.mapFiles) { // Map phase
		for i, task := range c.mapTasks {
			stale := task.status == ASSIGNED && (now-task.startTime) > c.timeout
			if task.status == UNASSIGNED || stale {
				log.Printf("Assgin map task %d (stale=%v)", i, stale)
				c.mapTasks[i].status = ASSIGNED
				c.mapTasks[i].startTime = now
				reply.TaskType = Map
				reply.TaskID = i
				reply.NumReduce = len(c.reduceTasks)
				reply.InputFile = c.mapFiles[i]
				return nil
			}
		}
	} else if c.numReduceDone < len(c.reduceTasks) { // Reduce phase
		for i, task := range c.reduceTasks {
			stale := task.status == ASSIGNED && (now-task.startTime) > c.timeout
			if task.status == UNASSIGNED || stale {
				log.Printf("Assgin reduce task %d (stale=%v)", i, stale)
				c.reduceTasks[i].status = ASSIGNED
				c.reduceTasks[i].startTime = now
				reply.TaskType = Reduce
				reply.TaskID = i
				reply.NumMap = len(c.mapTasks)
				return nil
			}
		}
	} else { // All tasks done
		reply.TaskType = Done
		return nil
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case Map:
		if c.mapTasks[args.TaskID].status != DONE {
			c.mapTasks[args.TaskID].status = DONE
			c.numMapDone += 1
			log.Printf("Map task %d done\n", args.TaskID)
		}
		if c.numMapDone == len(c.mapTasks) {
			log.Printf("Starting reduce phase with %d reducer tasks\n", len(c.reduceTasks))
		}
	case Reduce:
		if c.reduceTasks[args.TaskID].status != DONE {
			c.reduceTasks[args.TaskID].status = DONE
			c.numReduceDone += 1
			log.Printf("Reduce task %d done\n", args.TaskID)
		}
		if c.numReduceDone == len(c.reduceTasks) {
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
	return c.numMapDone == len(c.mapFiles) && c.numReduceDone == len(c.reduceTasks)
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
	c.numMapDone = 0
	c.mapTasks = make([]mrTask, len(files))
	c.mapFiles = make([]string, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = mrTask{status: UNASSIGNED, startTime: 0}
		c.mapFiles[i] = files[i]
	}
	c.numReduceDone = 0
	c.reduceTasks = make([]mrTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = mrTask{status: UNASSIGNED, startTime: 0}
	}
	c.timeout = 10
	c.server()
	return &c
}
