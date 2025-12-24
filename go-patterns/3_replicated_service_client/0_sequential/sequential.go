package sequential_rsc

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Args struct {
	Query string
	ID    int
}

type Reply struct {
	Data   string
	Server string
	Error  error
}

type ReplicatedClient interface {
	// Init initializes the client to use the given servers.
	//
	// Parameters:
	//   - servers: List of replica server identifiers (e.g., hostnames, URLs)
	//   - callOne: Function to make a single RPC call to a specific server
	//
	// The client handles server selection automatically:
	//   - Tries servers with timeouts (1 second per attempt)
	//   - Remembers which server responded fastest
	//   - Prefers that server for subsequent requests
	//   - Falls back to other servers if the preferred one times out
	//
	// This implements automatic failover and sticky routing to the fastest replica.
	Init(servers []string, callOne func(string, Args) Reply)

	// Call makes a request on any available server.
	// Multiple goroutines may call Call concurrently.
	Call(args Args) Reply
}

// 1. No shared state that we need to isolate apart from the previous server used.
// Mutex is fine for this
type Client struct {
	servers []string
	callOne func(string, Args) Reply
	mu      sync.Mutex
	prefer  int // Server ID of the preferred server
}

func (c *Client) Init(servers []string, callOne func(string, Args) Reply) {
	c.servers = servers
	c.callOne = callOne
}

// Naive version: tries servers in sequence with timeout
func (c *Client) Call(args Args) Reply {
	type result struct {
		serverID int
		reply    Reply
	}

	const timeout = 1 * time.Second
	t := time.NewTimer(timeout)
	// 1. When we create a timer with time.NewTimer(timeout), Go starts an internal goroutine that:
	// - Waits for the timeout duration
	// - Then sends the current time to the channel t.C
	//
	// If return early (before the timeout fires), that internal goroutine keeps running until the
	// timer expires, even though nobody is listening anymore.
	//
	// In a busy service, if we call this function 1000s of times per second, we would accumulate
	// 100,000+ timer goroutines waiting to expire. All these timer objects can't be GC'd (because
	// the internal  timer goroutine still holds a reference). Can also cause goroutine exhaustion (CPU overload from context switching)
	defer t.Stop()

	done := make(chan result, len(c.servers))

	// Try each server in sequence
	for off := range c.servers {
		id := (c.prefer + off) % len(c.servers) // start with preferred server
		server := c.servers[id]
		fmt.Printf("Trying %s...\n", server)

		go func() {
			reply := c.callOne(server, args)
			done <- result{id, reply}
		}()

		select {
		case r := <-done:
			return r.reply
		case <-t.C:
			t.Reset(timeout) // This server timed-out; reset timer and try next server
		}
	}

	// Fallback: All servers timed out, wait for at least one to finish
	r := <-done
	return r.reply
}

// Mock server call function - simulates network delays and failures
func mockCallOne(server string, args Args) Reply {
	// Simulate different server behaviors
	var delay time.Duration

	switch server {
	case "S0":
		delay = 2500 * time.Millisecond // Slow (will timeout)
	case "S1":
		delay = 300 * time.Millisecond // Fast
	default:
		delay = 1500 * time.Millisecond
	}

	// Add jitter
	delay += time.Duration(rand.Intn(100)) * time.Millisecond

	time.Sleep(delay)

	return Reply{
		Data:   fmt.Sprintf("Response to '%s' from %s", args.Query, server),
		Server: server,
		Error:  nil,
	}
}

func Main() {

	// Create client with multiple replica servers
	servers := []string{
		"S0", // 2500ms (slowest)
		"S1", // 300ms (fastest)
	}

	client := &Client{prefer: 0}
	client.Init(servers, mockCallOne)

	var wg sync.WaitGroup

	// Launch 10 concurrent requests!
	for i := range 10 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			req := Args{Query: fmt.Sprintf("Q%d", i), ID: i}
			reply := client.Call(req)

			fmt.Printf("[Request %d] Response by: %s\n", i, reply.Server)
		}()
	}

	wg.Wait()
	fmt.Printf("\nFinal preferred server: S%d\n", client.prefer)
}
