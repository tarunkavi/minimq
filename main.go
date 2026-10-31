package main

import (
	"io"
	"log"
	"net"
)

// 5672 is the standard AMQP port that RabbitMQ listens on.
const listenAddr = ":5672"

func main() {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", listenAddr, err)
	}
	defer listener.Close()
	log.Printf("minimq listening on %s", listenAddr)

	for {
		// Accept blocks until a client connects.
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}

		// Each connection gets its own goroutine, so a slow client
		// never stops the loop from accepting the next one.
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	addr := conn.RemoteAddr()
	log.Printf("client connected: %s", addr)

	buf := make([]byte, 4096)
	for {
		// Read blocks until the client sends bytes or closes the connection.
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
