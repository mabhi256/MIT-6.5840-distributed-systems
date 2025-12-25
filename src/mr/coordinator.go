package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu                   sync.Mutex
	nMap                 int
	nReduce              int
	mapTasksCompleted    int
	reduceTasksCompleted int
	done                 bool

	tasks   chan *Job
	pending map[int]*Job
}

const TIME_OUT = 10 * time.Second

func (c *Coordinator) ScheduleJob(args *JobReq, reply *JobReply) error {
	c.mu.Lock()

	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	select {
	case task := <-c.tasks:
		c.pending[task.JobID] = task
		c.mu.Unlock()

		task.WorkerID = args.WorkerID
		task.Timeout = time.Now().Add(TIME_OUT)

		reply.Job = *task

	default:
		// No tasks available
		var jobType JobType
		if c.done {
			jobType = EXIT_JOB
		} else {
			jobType = WAIT_JOB
		}
		c.mu.Unlock()

		reply.Job = Job{JobType: jobType}
	}

	return nil
}

func (c *Coordinator) NotifyDone(args *DoneReq, reply *DoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ignore stale jobs. Either it is no longer pending or the job types dont match
	task, exists := c.pending[args.JobID]
	if !exists || task.JobType != args.JobType {
		return nil
	}
	delete(c.pending, task.JobID)

	switch args.JobType {
	case MAP_JOB:
		c.mapTasksCompleted++

		if c.mapTasksCompleted == c.nMap { // all map jobs done
			c.pending = map[int]*Job{} // Reset map, just to be sure

			// Queue reduce tasks
			for i := range c.nReduce {
				c.tasks <- &Job{
					JobID:   i,
					JobType: REDUCE_JOB,
				}
			}
		}
	case REDUCE_JOB:
		c.reduceTasksCompleted++

		if c.reduceTasksCompleted == c.nReduce {
			c.done = true
		}
	}

	return nil
}

func (c *Coordinator) checkTimeouts() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		timeOutTasks := []*Job{}
		c.mu.Lock()
		if c.done {
			c.mu.Unlock()
			return // Exit the goroutine
		}

		for i, task := range c.pending {
			if task.Timeout.Before(time.Now()) {
				delete(c.pending, i)
				timeOutTasks = append(timeOutTasks, task)
			}
		}
		c.mu.Unlock()

		for _, task := range timeOutTasks {
			c.tasks <- task
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	done := c.done
	c.mu.Unlock()

	return done
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	taskLen := max(len(files), nReduce)

	c := Coordinator{
		nMap:    len(files),
		nReduce: nReduce,

		tasks:   make(chan *Job, taskLen),
		pending: map[int]*Job{},
	}

	// Queue map tasks
	for i, filename := range files {
		c.tasks <- &Job{
			JobID:    i,
			JobType:  MAP_JOB,
			Filename: filename,
		}
	}

	go c.checkTimeouts()

	c.server()
	return &c
}
