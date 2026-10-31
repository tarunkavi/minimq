package main

import (
	"io"
	"log"
	"net"
)

// Server accepts TCP connections and handles each client in its own goroutine.
type Server struct {
	addr     string
	listener net.Listener
}

func NewServer(addr string) *Server {
	return &Server{addr: addr}
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

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	addr := conn.RemoteAddr()
	log.Printf("client connected: %s", addr)

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			log.Printf("from %s: %d bytes: %q", addr, n, buf[:n])
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("read from %s: %v", addr, err)
			}
			break
		}
	}

	log.Printf("client disconnected: %s", addr)
}
