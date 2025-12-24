package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Msg struct {
	Tag  int64
	Data string
}

// gRPC uses HTTP/2 underneath, so it inherently uses protocol multiplexing
// ProtocolMux enables multiple RPC calls over one connection

type ProtocolMux interface {
	// Init initializes the mux to manage messages to the given service.
	Init(Service)
	// Call makes a request with the given message and returns the reply.
	// Multiple goroutines may call Call concurrently.
	Call(Msg) Msg
}

type Service interface {
	// ReadTag returns the muxing identifier in the request or reply message.
	// Multiple goroutines may call ReadTag concurrently.
	ReadTag(Msg) int64
	// Send sends a request message to the remote service.
	// Send must not be called concurrently with itself.
	Send(Msg)
	// Recv waits for and returns a reply message from the remote service.
	// Recv must not be called concurrently with itself.
	Recv() Msg
}

type Mux struct {
	srv  Service
	send chan Msg

	mu      sync.Mutex
	pending map[int64]chan<- Msg
}

func (m *Mux) Init(srv Service) {
	m.srv = srv
	m.send = make(chan Msg)
	m.pending = make(map[int64]chan<- Msg)
	go m.sendLoop()
	go m.recvLoop()
}

// Serialized - We are sending 1 send request at a time
func (m *Mux) sendLoop() {
	for args := range m.send {
		m.srv.Send(args)
	}
}

// Serialized - We are receiving 1 recv response at a time
func (m *Mux) recvLoop() {
	for {
		reply := m.srv.Recv()
		tag := m.srv.ReadTag(reply)

		m.mu.Lock()
		done := m.pending[tag]
		delete(m.pending, tag)
		m.mu.Unlock()

		if done == nil {
			panic("unexpected reply")
		}
		done <- reply
	}
}

func (m *Mux) Call(args Msg) (reply Msg) {
	tag := m.srv.ReadTag(args)
	done := make(chan Msg, 1)

	m.mu.Lock()
	if m.pending[tag] != nil {
		m.mu.Unlock()
		panic("mux: duplicate call tag")
	}
	m.pending[tag] = done
	m.mu.Unlock()

	m.send <- args
	return <-done
}

// Simulating a "TCP connection" - shared buffer that gets corrupted if accessed concurrently
type MockService struct {
	sendBuffer []byte
	recvBuffer []byte

	pendingReplies chan []byte // the wire
}

func NewMockService() *MockService {
	s := &MockService{
		sendBuffer:     make([]byte, 0, 1024),
		recvBuffer:     make([]byte, 0, 1024),
		pendingReplies: make(chan []byte, 10),
	}

	return s
}

func (s *MockService) ReadTag(m Msg) int64 {
	return m.Tag
}

// CLIENT SIDE: Encode and send
func (s *MockService) Send(m Msg) {
	fmt.Printf("  [Send] called with tag %d\n", m.Tag)

	// Encode to JSON bytes
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(m)

	// This is where corruption happens with concurrent calls
	// Multiple goroutines would interleave their writes
	time.Sleep(1 * time.Millisecond)
	s.sendBuffer = append(s.sendBuffer, buf.Bytes()...)
	time.Sleep(1 * time.Millisecond)

	// "Send over network to server"
	s.flushSend()
}

// CLIENT SIDE: Receive and decode response
func (s *MockService) Recv() Msg {
	// Get encoded bytes from "network"
	encodedReply := <-s.pendingReplies

	time.Sleep(1 * time.Millisecond)
	s.recvBuffer = append(s.recvBuffer, encodedReply...)
	time.Sleep(1 * time.Millisecond)

	// Decode the JSON bytes
	var reply Msg
	buf := bytes.NewBuffer(s.recvBuffer)
	json.NewDecoder(buf).Decode(&reply)
	s.recvBuffer = s.recvBuffer[:0]

	fmt.Printf("  [Recv] returning tag %d\n", reply.Tag)
	return reply
}

// SERVER SIDE: Receive request, process, send back encoded reply
func (s *MockService) flushSend() {
	if len(s.sendBuffer) <= 0 {
		return
	}

	// Server receives and decodes the request
	var request Msg
	buf := bytes.NewBuffer(s.sendBuffer)
	json.NewDecoder(buf).Decode(&request)
	s.sendBuffer = s.sendBuffer[:0]

	// Server processes and creates reply
	go func(req Msg) {
		time.Sleep(time.Duration(100+rand.IntN(50)) * time.Millisecond)

		reply := Msg{
			Tag:  req.Tag,
			Data: "Reply: " + req.Data,
		}

		// Server ENCODES the reply to JSON bytes
		replyBuf := new(bytes.Buffer)
		json.NewEncoder(replyBuf).Encode(reply)

		// Send encoded bytes back to client
		s.pendingReplies <- replyBuf.Bytes()
	}(request)
}

func main() {
	service := NewMockService()
	mux := &Mux{}
	mux.Init(service)

	var wg sync.WaitGroup

	// 3 clients making concurrent requests
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for reqNum := 1; reqNum <= 3; reqNum++ {
				tag := int64(i*100 + reqNum)

				fmt.Printf("Client %d: sending request #%d (tag %d)\n", i, reqNum, tag)

				// Direct call - WILL CAUSE RACES
				// Run with: go run -race main.go
				// service.Send(Msg{
				// 	Tag:  tag,
				// 	Data: fmt.Sprintf("Client%d-Req%d", i, reqNum),
				// })

				reply := mux.Call(Msg{
					Tag:  tag,
					Data: fmt.Sprintf("Client%d-Req%d", i, reqNum),
				})

				fmt.Printf("Client %d: got reply for #%d: %q\n", i, reqNum, reply.Data)
			}
		}()
	}

	wg.Wait()
}
