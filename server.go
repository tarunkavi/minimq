package main

import (
	"log"
	"net"
)

type Server struct {
	addr     string
	listener net.Listener
	queue    *Queue
}

func NewServer(addr string) *Server {
	return &Server{
		addr:  addr,
		queue: NewQueue("default"),
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

	go s.queue.Run()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}

		go NewClient(conn, s.queue).handleConn()
	}
}
