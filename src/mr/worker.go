package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type IntermediateKV struct {
	Key   string
	Value []string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := os.Getpid()

	for {
		reply, err := CallScheduleJob(workerID)
		if err != nil {
			fmt.Printf("%s", err.Error())
		}

		switch reply.Job.JobType {
		case MAP_JOB:
			ExecuteMapJob(workerID, reply, mapf)

		case REDUCE_JOB:
			ExecuteReduceJob(workerID, reply, reducef)

		case WAIT_JOB:
			time.Sleep(time.Second)

		case EXIT_JOB:
			return
		}
	}
}

func CallScheduleJob(workerID int) (*JobReply, error) {
	jobReq := JobReq{
		WorkerID: workerID,
	}
	jobReply := JobReply{}

	ok := call("Coordinator.ScheduleJob", &jobReq, &jobReply)

	if !ok {
		return nil, fmt.Errorf("call failed: ScheduleJob")
	}

	return &jobReply, nil
}

func CallNotifyDone(workerID int, jobType JobType, jobID int) error {
	doneReq := DoneReq{
		WorkerID: workerID,
		JobType:  jobType,
		JobID:    jobID,
	}
	doneReply := DoneReply{}

	ok := call("Coordinator.NotifyDone", &doneReq, &doneReply)

	if !ok {
		return fmt.Errorf("call failed: NotifyDone")
	}

	return nil
}

func ExecuteMapJob(workerID int, reply *JobReply, mapf func(string, string) []KeyValue) {
	split, err := os.Open(reply.Job.Filename)
	if err != nil {
		log.Printf("cannot open %v", reply.Job.Filename)
	}
	defer split.Close()

	content, err := io.ReadAll(split)
	if err != nil {
		log.Printf("cannot read %v", reply.Job.Filename)
	}

	kva := mapf(reply.Job.Filename, string(content))

	intermediate := make(map[string][]KeyValue)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.NReduce
		filename := fmt.Sprintf("mr-%d-%d", reply.Job.JobID, reduceID)
		intermediate[filename] = append(intermediate[filename], kv)
	}

	for key, entries := range intermediate {
		temp, err := os.CreateTemp(".", key+"-*")
		if err != nil {
			log.Print(err.Error())
		}

		enc := json.NewEncoder(temp)
		for _, entry := range entries {
			err := enc.Encode(&entry)
			if err != nil {
				log.Print(err.Error())
			}
		}

		temp.Close()
		os.Rename(temp.Name(), key)
	}

	CallNotifyDone(workerID, MAP_JOB, reply.Job.JobID)
}

func ExecuteReduceJob(workerID int, reply *JobReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for i := range reply.NMap {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.Job.JobID)
		file, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				continue // No data for this partition
			}
			log.Printf("error opening %v: %v", filename, err)
			return // Fail the task
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reply.Job.JobID)

	ofile, err := os.CreateTemp(".", oname+"-*")
	if err != nil {
		log.Print(err.Error())
	}

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
	os.Rename(ofile.Name(), oname)

	CallNotifyDone(workerID, REDUCE_JOB, reply.Job.JobID)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
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
