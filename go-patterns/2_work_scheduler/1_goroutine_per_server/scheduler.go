package goroutine_per_server

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Problem:  If numTask = 1,000,000 but servers = 10, we create 1,000,000 goroutines
// Most goroutines sit blocked waiting for a server
// Goroutines constantly created and destroyed
// Memory overhead proportional to number of tasks
// Hint #: Think carefully before introducing unbounded queuing
func Schedule(servers []string, numTask int, call func(srv string, task int)) {
	// 1. Instead of goroutines per task pulling idle servers and scheduling their task,
	// We need goroutines per server to PULL TASKS from a queue
	tasks := make(chan int) // queue of tasks to do
	done := make(chan bool) // 2. Need a reply channel (true per server) to know our task is done

	runTasks := func(srv string) {
		for task := range tasks {
			call(srv, task)
		}
		done <- true
	}

	for _, srv := range servers {
		go runTasks(srv)
	}

	// Queue all tasks
	for task := range numTask {
		tasks <- task
	}
	close(tasks) // Close means no more values will be sent. We can still read them

	for range servers {
		<-done // All servers are done
	}

}

func Main() {
	servers := []string{"S1", "S2", "S3"}
	numTasks := 100

	var wg sync.WaitGroup
	wg.Add(numTasks)

	// Mock function that simulates work
	call := func(srv string, task int) {
		defer wg.Done()
		fmt.Printf("Server %s processing task-%d\n", srv, task)
		jitter := 1250 - rand.IntN(500)                      // 1 +/- 0.25
		time.Sleep(time.Duration(jitter) * time.Millisecond) // Simulate work
	}

	Schedule(servers, numTasks, call)

	wg.Wait()
	fmt.Println("Done")
}
