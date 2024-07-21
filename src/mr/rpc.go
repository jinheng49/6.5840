package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

func (t *TaskInfo) GetFilename(num string) string {
	switch t.TaskType {
	case TaskMap:
		return fmt.Sprintf("%s-mr-out-%d-%s", t.TaskType, t.Number, num)
	case TaskReduce:
		return fmt.Sprintf("mr-out-%d", t.Number)
	default:
		return ""
	}
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
