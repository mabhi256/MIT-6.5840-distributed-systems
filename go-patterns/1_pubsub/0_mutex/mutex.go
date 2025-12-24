package mutex

import (
	"fmt"
	"sync"
	"time"
)

type Event struct {
	Message string
	ID      int
}

type Server struct {
	mu  sync.Mutex
	sub map[chan<- Event]bool
}

func (s *Server) Init() {
	s.sub = make(map[chan<- Event]bool)
}

func (s *Server) Publish(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.sub {
		c <- e
	}
}

func (s *Server) Subscribe(c chan<- Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub[c] {
		panic("pubsub: already subscribed")
	}
	s.sub[c] = true
	fmt.Printf("[Server] New subscriber registered (total: %d)\n", len(s.sub))
}

func (s *Server) Cancel(c chan<- Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.sub[c] {
		panic("pubsub: not subscribed")
	}
	close(c)
	delete(s.sub, c)
	fmt.Printf("[Server] Subscriber cancelled (total: %d)\n", len(s.sub))
}

func Main() {
	server := &Server{}
	server.Init()

	// Fast subscriber - processes immediately
	fastSub := make(chan Event, 10)
	server.Subscribe(fastSub)

	go func() {
		for e := range fastSub {
			fmt.Printf("  [Fast Subscriber] Processed event %d: %s\n", e.ID, e.Message)
		}
	}()

	// Slow subscriber - takes 2 seconds per event, UNBUFFERED channel
	slowSub := make(chan Event) // NO BUFFER!
	server.Subscribe(slowSub)

	go func() {
		for e := range slowSub {
			time.Sleep(2 * time.Second) // Simulate slow processing
			fmt.Printf("  [Slow Subscriber] Processed event %d\n", e.ID)
		}
	}()

	// Give subscribers time to start
	time.Sleep(100 * time.Millisecond)

	go func() {
		time.Sleep(500 * time.Millisecond) // Wait until middle of event 2 publish

		fmt.Println("\n>>> [NEW SUB] Trying to subscribe now...")
		newSub := make(chan Event, 10)

		start := time.Now()
		server.Subscribe(newSub) // This will block!
		duration := time.Since(start)

		fmt.Printf(">>> [NEW SUB] Finally subscribed after %v\n\n", duration)
	}()

	// Try to publish 3 events
	fmt.Println()
	fmt.Println("=== Starting to publish events ===")
	fmt.Println()

	for i := 1; i <= 3; i++ {
		fmt.Printf("\n--- Attempting to publish event %d ---\n", i)
		start := time.Now()

		server.Publish(Event{
			Message: fmt.Sprintf("Event number %d", i),
			ID:      i,
		})

		duration := time.Since(start)
		fmt.Printf("--- Publish %d took %v ---\n\n", i, duration)
	}

	fmt.Println()
	fmt.Println("=== All publishes completed ===")
	time.Sleep(3 * time.Second)
}
