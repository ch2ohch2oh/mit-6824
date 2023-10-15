package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type TaskType string

const (
	Map    TaskType = "map"
	Reduce TaskType = "reduce"
	Done   TaskType = "done"
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType  TaskType
	TaskID    int
	NumMap    int    // Number of map files - used by the reducer to determine input files
	NumReduce int    // Number of reduce files - used by the mapper to determine output files
	InputFile string // Input file path for the mapper
}

type FinishTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
