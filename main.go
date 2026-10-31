package main

import "log"

const listenAddr = ":5672"

func main() {
	server := NewServer(listenAddr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("listen on %s: %v", listenAddr, err)
	}
}
