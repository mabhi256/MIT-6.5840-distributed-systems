package channel2

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

func deliver(in <-chan Event, out chan<- Event) {
	var q []Event // Store events here

	// 3. Naive mailbox
	// for {
	// 	select {
	// 	case event := <-in:
	// 		q = append(q, event) // 3. Store received events
	//
	// 	case out <- q[0]: // 3. Send oldest event
	// 		q = q[1:] // 3. Remove it from queue
	// 	}
	// }

	// 4. q[0] will crash as soon as we start as q is empty.
	// If the mailbox is empty, sendOut is nil, so sendOut <- next will block
	// for {
	// 	   var sendOut chan<- Event
	// 	   var next Event
	// 	   if len(q) > 0 {
	// 		   sendOut = out // Enable sending
	// 		   next = q[0]
	// 	   }
	//
	// 	   select {
	// 	   case e := <-in:
	// 		   q = append(q, e)
	// 	   case sendOut <- next:
	// 		   q = q[1:]
	// 	   }
	// }

	// 5. Need to handle cancel events - input channel will send nil when it closes
	for in != nil || len(q) > 0 {
		// Exit the loop when input channel is closed and the queue is flushed
		var sendOut chan<- Event
		var next Event
		if len(q) > 0 {
			sendOut = out // Enable sending
			next = q[0]
		}

		select {
		case e, ok := <-in:
			if !ok {
				in = nil // stop receiving from in
				break
			}
			q = append(q, e)
		case sendOut <- next:
			q = q[1:]
		}
	}
	close(out)
}

func (s *Server) loop() {
	// 3. Instead of sending data to the subcriber directly, send it to its 'mailbox'
	// subscribers := make(map[chan<- Event]bool) // Set of subscribers
	mailboxMap := make(map[chan<- Event]chan<- Event)

	for {
		select {
		case event := <-s.publish:
			// 1. This blocks until someone receives
			// 	for sub := range subscribers {
			//  	sub <- event
			//  }
			// 2. Doesn't block but we spawn a goroutine per event per subscriber
			// 	1000 events × 100 subscribers = 100,000 goroutines!
			// go func(s chan<- Event) {
			// 	s <- event
			// }(sub)
			for _, mailbox := range mailboxMap {
				mailbox <- event
			}
		case req := <-s.subscribe:
			mailbox := make(chan Event) // 3. Create mailbox channel for each subscriber

			if mailboxMap[req.sub] != nil {
				req.ok <- false
				break
			}
			// 3. Instead of spawning goroutine for each event for each subscriber in 'case event := <-s.publish'
			// Spawn the goroutine HERE, once per subscriber when we subscribe
			go deliver(mailbox, req.sub)
			mailboxMap[req.sub] = mailbox
			req.ok <- true
		case req := <-s.cancel:
			if mailboxMap[req.sub] != nil {
				req.ok <- false
				break
			}
			mailbox := mailboxMap[req.sub]
			close(mailbox)
			delete(mailboxMap, req.sub)
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
	time.Sleep(10 * time.Second)
}
