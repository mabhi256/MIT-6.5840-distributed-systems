package channel1

import (
	"fmt"
	"time"
)

type Event struct {
	Message string
	ID      int
}

type subReq struct {
	sub chan<- Event // The subscription you want to add
	ok  chan bool    // The reply channel for the request
}

type Server struct {
	publish   chan Event
	subscribe chan subReq
	cancel    chan subReq
}

func (s *Server) Init() {
	s.publish = make(chan Event)
	s.subscribe = make(chan subReq)
	s.cancel = make(chan subReq)
	go s.loop()
}

func (s *Server) loop() {
	subscribers := make(map[chan<- Event]bool)

	for {
		select {
		case event := <-s.publish:
			for sub := range subscribers {
				// This will still block if subscriber is slow
				sub <- event
			}
		case req := <-s.subscribe:
			if subscribers[req.sub] {
				req.ok <- false
				break
			}
			subscribers[req.sub] = true
			req.ok <- true
		case req := <-s.cancel:
			if subscribers[req.sub] {
				req.ok <- false
				break
			}
			close(req.sub)
			delete(subscribers, req.sub)
			req.ok <- true
		}
	}
}

func (s *Server) Publish(e Event) {
	s.publish <- e
}

func (s *Server) Subscribe(c chan<- Event) {
	req := subReq{sub: c, ok: make(chan bool)}
	s.subscribe <- req
	if !<-req.ok {
		panic("pubsub: already subscribed")
	}
}

func (s *Server) Cancel(c chan<- Event) {
	req := subReq{sub: c, ok: make(chan bool)}
	s.cancel <- req
	if !<-req.ok {
		panic("pubsub: not subscribed")
	}
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
