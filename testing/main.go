package main

import (
	"flag"
	"log"
	"net"

	"github.com/go-stomp/stomp/v3/frame"
)

func main() {
	addr := flag.String("addr", "localhost:5672", "server address")
	destination := flag.String("dest", "/queue/test", "destination to send to")
	body := flag.String("body", "hello from the test client", "message body")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("connect to %s: %v", *addr, err)
	}
	defer conn.Close()

	f := frame.New(frame.SEND,
		frame.Destination, *destination,
		frame.ContentType, "text/plain",
	)
	f.Body = []byte(*body)

	if err := frame.NewWriter(conn).Write(f); err != nil {
		log.Fatalf("send frame: %v", err)
	}
	log.Printf("sent SEND to %s: %q", *destination, *body)
}
