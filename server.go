package main

import (
	"io"
	"log"
	"net"

	"github.com/go-stomp/stomp/v3/frame"
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

		s.handleFrame(addr, f)
	}

	log.Printf("client disconnected: %s", addr)
}

func (s *Server) handleFrame(addr net.Addr, f *frame.Frame) {
	switch f.Command {
	case frame.SEND:
		s.queue.mailbox <- QueueOp{Kind: OpEnqueue, Body: f.Body}
	case frame.SUBSCRIBE:
		log.Printf("from %s: SUBSCRIBE not implemented", addr)
	default:
		log.Printf("from %s: operation not known: %s", addr, f.Command)
	}
}
