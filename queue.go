package main

import (
	"log"
	"math/rand/v2"
)

type OpKind int

const (
	OpEnqueue OpKind = iota
	OpSubscribe
	OpUnsubscribe
)

type QueueOp struct {
	Kind OpKind
	Body []byte
	Name string
	Sub  *Subscriber
}

type Queue struct {
	name     string
	messages [][]byte
	mailbox  chan QueueOp
	subs     []*Subscriber
}

type Subscriber struct {
	id          string
	destination string
	outputData  chan []byte
}

func (s *Subscriber) send(data []byte) {
	s.outputData <- data
}

func NewQueue(name string) *Queue {
	return &Queue{
		name:    name,
		mailbox: make(chan QueueOp),
	}
}

func (q *Queue) Run() {
	for op := range q.mailbox {
		switch op.Kind {
		case OpEnqueue:
			log.Printf("queue %s received: %q", q.name, op.Body)
			q.Enqueue(op.Body)
			q.deliver()
		case OpSubscribe:
			q.subs = append(q.subs, op.Sub)
			q.deliver()
			log.Printf("queue %s: subscriber added, %d total", q.name, len(q.subs))
		case OpUnsubscribe:
			q.removeSub(op.Sub)
			close(op.Sub.outputData)
			log.Printf("queue %s: subscriber removed, %d total", q.name, len(q.subs))
		default:
			log.Printf("queue %s: unknown op %d", q.name, op.Kind)
		}
	}
}

func (q *Queue) deliver() {
	for len(q.messages) > 0 && len(q.subs) > 0 {
		message, _ := q.Dequeue()
		sub := q.subs[rand.IntN(len(q.subs))] // make it roundrobin afterwords
		sub.send(message)
	}
}

// optimization for remove from slice. since a direct remove clause data reallocation of the next n elements
func (q *Queue) removeSub(sub *Subscriber) {
	for i, s := range q.subs {
		if s == sub {
			last := len(q.subs) - 1
			q.subs[i] = q.subs[last]
			q.subs[last] = nil
			q.subs = q.subs[:last]
			return
		}
	}
}

func (q *Queue) Enqueue(msg []byte) {
	q.messages = append(q.messages, msg)
}

func (q *Queue) Dequeue() ([]byte, bool) {
	if len(q.messages) == 0 {
		return nil, false
	}
	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg, true
}
