package main

import (
	"io"
	"log"
	"net"
)

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

	codec := NewStompCodec(conn)
	for {
		f, err := codec.ReadFrame()
		if err != nil {
			if err != io.EOF {
				log.Printf("read from %s: %v", addr, err)
			}
			break
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
