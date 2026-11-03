package main

import (
	"log"
	"net"
	"sync"
)

type Server struct {
	addr     string
	listener net.Listener
	queues   map[string]*Queue
	mu       sync.RWMutex //lock to protect queus map.
}

func NewServer(addr string) *Server {
	return &Server{
		addr:   addr,
		queues: make(map[string]*Queue),
		mu:     sync.RWMutex{},
	}
}

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	defer listener.Close()
	log.Printf("minimq listening on %s", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}

		go NewClient(conn, s).handleConn()
	}
}

func (s *Server) getQueue(name string) (*Queue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.queues[name]
	return q, ok
}

// insertQueue creates the queue and starts its Run goroutine. If another client
// created it first, it returns that one instead of replacing it.
func (s *Server) insertQueue(name string) *Queue {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[name]; ok {
		return q
	}
	q := NewQueue(name)
	s.queues[name] = q
	go q.Run()
	return q
}
