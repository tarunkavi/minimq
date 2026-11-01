package main

import (
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/go-stomp/stomp/v3/frame"
)

type Client struct {
	conn     net.Conn
	codec    *StompCodec
	queue    *Queue
	outgoing chan *frame.Frame
	subs     map[string]*Subscriber
}

func NewClient(conn net.Conn, queue *Queue) *Client {
	return &Client{
		conn:     conn,
		codec:    NewStompCodec(conn),
		queue:    queue,
		outgoing: make(chan *frame.Frame),
		subs:     make(map[string]*Subscriber),
	}
}

func (c *Client) startWriter() {
	go func() {
		for f := range c.outgoing {
			if err := c.codec.WriteFrame(f); err != nil {
				log.Printf("write to %s: %v", c.conn.RemoteAddr(), err)
				return
			}
		}
	}()
}

func (c *Client) handleConn() {
	defer c.Close()

	addr := c.conn.RemoteAddr()
	log.Printf("client connected: %s", addr)

	c.startWriter()

	for {
		f, err := c.codec.ReadFrame()
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

		c.handleFrame(f)
	}

	log.Printf("client disconnected: %s", addr)
}
func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) handleFrame(f *frame.Frame) {
	addr := c.conn.RemoteAddr()
	switch f.Command {
	case frame.SEND:
		c.queue.mailbox <- QueueOp{Kind: OpEnqueue, Body: f.Body}
	case frame.SUBSCRIBE:
		id := f.Header.Get(frame.Id)
		destination := f.Header.Get(frame.Destination)
		name := strings.TrimPrefix(destination, "/queue/")
		sub := &Subscriber{
			id:          id,
			destination: destination,
			outputData:  make(chan []byte),
		}
		c.subs[id] = sub
		go c.forward(sub)
		c.queue.mailbox <- QueueOp{Kind: OpSubscribe, Body: f.Body, Name: name, Sub: sub}
		log.Printf("from %s: SUBSCRIBE not implemented", addr)
	case frame.UNSUBSCRIBE:
		id := f.Header.Get(frame.Id)
		sub, ok := c.subs[id]
		if !ok {
			log.Printf("from %s: no subscription with id %q", addr, id)
			return
		}
		c.queue.mailbox <- QueueOp{Kind: OpUnsubscribe, Sub: sub}
		delete(c.subs, id)
	default:
		log.Printf("from %s: operation not known: %s", addr, f.Command)
	}
}

func (c *Client) forward(sub *Subscriber) {
	count := 0
	for data := range sub.outputData { //this is read only hence no need of any mutex for the writes in handleFrame.
		f := frame.New(frame.MESSAGE,
			frame.Subscription, sub.id,
			frame.Destination, sub.destination,
			frame.MessageId, sub.id+"-"+strconv.Itoa(count),
		)
		count++
		f.Body = data
		c.outgoing <- f
	}
}
