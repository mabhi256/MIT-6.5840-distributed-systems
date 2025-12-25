package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type JobType int

const (
	MAP_JOB JobType = iota
	REDUCE_JOB
	WAIT_JOB
	EXIT_JOB
)

// type JobStatus int

// const (
// 	IDLE JobStatus = iota
// 	IN_PROGRESS
// 	COMPLETED
// )

type Job struct {
	WorkerID int
	JobID    int
	JobType  JobType
	// JobStatus JobStatus
	Filename string
	Timeout  time.Time
}

type JobReq struct {
	WorkerID int
}

type JobReply struct {
	Job     Job
	NMap    int
	NReduce int
}

type DoneReq struct {
	WorkerID int
	JobType  JobType
	JobID    int
}

type DoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
