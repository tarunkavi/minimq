package main

import "log"

// 5672 is the standard AMQP port that RabbitMQ listens on.
const listenAddr = ":5672"

func main() {
	server := NewServer(listenAddr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("listen on %s: %v", listenAddr, err)
	}
}
