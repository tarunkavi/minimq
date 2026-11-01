package main

import (
	"flag"
	"io"
	"log"
	"net"

	"github.com/go-stomp/stomp/v3/frame"
)

func main() {
	addr := flag.String("addr", "localhost:5672", "server address")
	destination := flag.String("dest", "/queue/test", "destination to subscribe to")
	id := flag.String("id", "sub-1", "subscription id")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("connect to %s: %v", *addr, err)
	}
	defer conn.Close()

	f := frame.New(frame.SUBSCRIBE,
		frame.Id, *id,
		frame.Destination, *destination,
		frame.Ack, frame.AckAuto,
	)
	if err := frame.NewWriter(conn).Write(f); err != nil {
		log.Fatalf("send SUBSCRIBE: %v", err)
	}
	log.Printf("subscribed to %s as %s, waiting for messages", *destination, *id)

	reader := frame.NewReader(conn)
	for {
		f, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				log.Fatalf("read: %v", err)
			}
			log.Printf("server closed the connection")
			return
		}
		if f == nil {
			continue
		}
		log.Printf("received %s: %q", f.Command, f.Body)
	}
}
