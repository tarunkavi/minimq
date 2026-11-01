package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"time"

	"github.com/go-stomp/stomp/v3/frame"
)

var addr = flag.String("addr", "localhost:5672", "server address")

const dest = "/queue/unsub-test"

type conn struct {
	net.Conn
	r *frame.Reader
	w *frame.Writer
}

func dial() *conn {
	c, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("connect to %s: %v", *addr, err)
	}
	return &conn{c, frame.NewReader(c), frame.NewWriter(c)}
}

func (c *conn) write(f *frame.Frame) {
	if err := c.w.Write(f); err != nil {
		log.Fatalf("write %s: %v", f.Command, err)
	}
}

func (c *conn) subscribe(id string) {
	c.write(frame.New(frame.SUBSCRIBE, frame.Id, id, frame.Destination, dest))
}

func (c *conn) unsubscribe(id string) {
	c.write(frame.New(frame.UNSUBSCRIBE, frame.Id, id))
}

// collect reads MESSAGE frames for d and returns them as "subscription:body".
func (c *conn) collect(d time.Duration) []string {
	var got []string
	c.SetReadDeadline(time.Now().Add(d))
	for {
		f, err := c.r.Read()
		if err != nil {
			return got
		}
		if f != nil {
			got = append(got, f.Header.Get(frame.Subscription)+":"+string(f.Body))
		}
	}
}

func send(body string) {
	c := dial()
	defer c.Close()
	f := frame.New(frame.SEND, frame.Destination, dest)
	f.Body = []byte(body)
	c.write(f)
}

var failed bool

func check(name string, got, want []string) {
	if slices.Equal(got, want) {
		fmt.Printf("PASS: %s\n", name)
	} else {
		fmt.Printf("FAIL: %s: got %v, want %v\n", name, got, want)
		failed = true
	}
}

func main() {
	flag.Parse()
	wait := 200 * time.Millisecond

	c := dial()
	defer c.Close()
	c.subscribe("s1")
	c.subscribe("s2")
	time.Sleep(wait)
	c.unsubscribe("s1")
	c.unsubscribe("unknown")
	time.Sleep(wait)
	for i := 1; i <= 4; i++ {
		send(fmt.Sprintf("m%d", i))
	}
	check("after unsubscribing s1, only s2 gets messages", c.collect(time.Second),
		[]string{"s2:m1", "s2:m2", "s2:m3", "s2:m4"})

	c.unsubscribe("s2")
	time.Sleep(wait)
	send("waiting")
	check("with no subscribers left, nothing is delivered", c.collect(500*time.Millisecond), nil)

	c2 := dial()
	defer c2.Close()
	c2.subscribe("s3")
	check("a new subscriber gets the waiting message", c2.collect(500*time.Millisecond),
		[]string{"s3:waiting"})

	// Unsubscribe before closing: a subscriber that just disconnects stays in the queue.
	c2.unsubscribe("s3")
	time.Sleep(wait)

	if failed {
		os.Exit(1)
	}
}
