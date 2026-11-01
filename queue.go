package main

import "log"

type OpKind int

const (
	OpEnqueue OpKind = iota
)

type QueueOp struct {
	Kind OpKind
	Body []byte
}

type Queue struct {
	name     string
	messages [][]byte
	mailbox  chan QueueOp
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
		default:
			log.Printf("queue %s: unknown op %d", q.name, op.Kind)
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
