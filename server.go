package main

import (
	"io"
	"log"
	"net"

	"github.com/go-stomp/stomp/v3/frame"
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

	reader := frame.NewReader(conn)
	for {
		f, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				log.Printf("read from %s: %v", addr, err)
			}
			break
		}
		// A nil frame with no error is a heart-beat (an empty line).
		if f == nil {
			continue
		}

		log.Printf("from %s: command=%s", addr, f.Command)
		for i := 0; i < f.Header.Len(); i++ {
			key, value := f.Header.GetAt(i)
			log.Printf("  header %s=%s", key, value)
		}
		log.Printf("  body %q", f.Body)
	}

	log.Printf("client disconnected: %s", addr)
}
