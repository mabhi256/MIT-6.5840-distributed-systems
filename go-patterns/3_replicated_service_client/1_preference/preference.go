package preference_rsc

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
	Init(servers []string, callOne func(string, Args) Reply)
	Call(args Args) Reply
}

// Hint 1#: Use a mutex if that is the clearest way to write the code.
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

func (c *Client) Call(args Args) Reply {
	type result struct {
		serverID int
		reply    Reply
	}

	const timeout = 1 * time.Second
	t := time.NewTimer(timeout)
	defer t.Stop() // Hint 2#: Stop timers you don’t need.

	done := make(chan result, len(c.servers))

	// Hint 4#: Without mutex, go run -race main.go will show us data race
	c.mu.Lock()
	prefer := c.prefer
	c.mu.Unlock()

	var r result
	for off := range c.servers {
		id := (prefer + off) % len(c.servers) // start with preferred server
		server := c.servers[id]
		fmt.Printf("Trying %s...\n", server)

		go func() {
			reply := c.callOne(server, args)
			done <- result{id, reply}
		}()

		select {
		case r = <-done:
			goto Done
		case <-t.C:
			t.Reset(timeout)
		}
	}

	// Fallback: All servers timed out, wait for at least one to finish
	r = <-done

	// Hint 3#: Use a goto if that is the clearest way to write the code
Done:
	c.mu.Lock()
	c.prefer = r.serverID
	c.mu.Unlock()

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
