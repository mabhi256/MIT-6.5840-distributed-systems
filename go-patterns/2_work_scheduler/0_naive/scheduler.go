package naive_scheduler

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Problem: We need to distribute numTask tasks across multiple servers,
// where each server can handle one task at a time.

// Hint #1: Use a buffered channel as a concurrent blocking queue
func Schedule(servers []string, numTask int, call func(srv string, task int)) {
	// The idle channel holds available servers.
	// It's buffered to hold all servers initially.
	idle := make(chan string, len(servers))
	for _, srv := range servers {
		idle <- srv
	}

	// Hint #2: Each task runs in its own goroutine,
	for task := range numTask {
		srv := <-idle // 5. Independently grab an idle server from the pool and then spin-off goroutine

		// Hint #3: Closure Variable Capture Bug
		// The closure that's running as a new goroutine refers to the loop iteration variable 'task'
		// By the time the goroutine starts exiting, the loop might have progressed and done a task++
		// and we may end up skipping some values. So we need to capture local value like
		// go func(task int) {...}(task)
		// Fixed in Go 1.22, so this will work as expected and dont need to do anything extra
		go func() {
			// 5. Instead of spawning a goroutine and then wait for an idle server,
			// first wait then spawn
			// srv := <-idle
			call(srv, task)
			idle <- srv // Move the server to the idle pool after the task is done
		}()
	}

	for range servers {
		<-idle // 6. If we can pull all the idle servers out, this means all tasks are completed
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
