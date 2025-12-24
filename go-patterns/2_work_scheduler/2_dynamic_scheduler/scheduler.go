package dynamic_scheduler

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Problem: Servers may not be fixed. It needs to be a channel so that we can send server values dynamically
// Even tasks can fail, so we have to retry them
func Schedule(servers chan string, numTask int, call func(srv string, task int) bool) {
	tasks := make(chan int, numTask)
	done := make(chan bool)
	exit := make(chan bool)

	runTasks := func(srv string) {
		for task := range tasks {
			if call(srv, task) {
				done <- true
			} else {
				tasks <- task
			}
		}
		// done <- true
	}

	go func() {
		// 1. loop waits for servers on the channel
		// for srv := range servers { // 4. This goroutine runs FOREVER
		// 	go runTasks(srv) // spawn a worker for each server
		// }

		for {
			select {
			case srv := <-servers:
				go runTasks(srv)
			case <-exit:
				return // Exit this goroutine
			}
		}
	}()

	// Queue all tasks
	for task := range numTask {
		tasks <- task
	}
	// close(tasks) // 3. Since we need to requeue failed tasks, we cannot close the channel here

	// 2. Since the servers are dynamic, we can't wait for servers to finish, this list would change
	// for range servers {
	for range numTask {
		<-done // Wait for all tasks to complete
	}
	close(tasks) // 3. Close AFTER all tasks succeed

	// 4. Schedule returns, but goroutine still waiting on servers. So we need to add exit signal channel
	exit <- true // Signal goroutine to exit
}

func Main() {
	serverChan := make(chan string, 10) // Buffer for adding servers

	numTasks := 30

	var wg sync.WaitGroup
	wg.Add(numTasks)

	// Mock function that simulates work
	call := func(srv string, task int) bool {
		status := rand.IntN(5) > 0 // 3/4 chance of failure
		statusStr := "Failed"
		if status {
			statusStr = "Success"
			defer wg.Done()
		}

		fmt.Printf("Server %s processing task-%d (%s)\n", srv, task, statusStr)
		jitter := 1250 - rand.IntN(500)                      // 1±0.25 sec
		time.Sleep(time.Duration(jitter) * time.Millisecond) // Simulate work
		return status
	}

	// Add initial servers
	fmt.Println("Adding S1...")
	serverChan <- "S1"
	fmt.Println("Adding S2...")
	serverChan <- "S2"

	// Simulate adding servers dynamically during execution
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("Adding S3...")
		serverChan <- "S3"

		time.Sleep(2 * time.Second)
		fmt.Println("Adding S4...")
		serverChan <- "S4"
		// 5. Cannot close the server channel when the Schedule() is still running
		// The select case becomes immediately ready (doesn't block)
		// We receive the zero value ("") of the chan string
		// close(serverChan)
	}()

	Schedule(serverChan, numTasks, call)
	close(serverChan)

	wg.Wait()
	fmt.Println("Done")
}
