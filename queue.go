package main

import "log"

type Queue struct {
	name     string
	messages [][]byte
	mailbox  chan []byte
}

func NewQueue(name string) *Queue {
	return &Queue{
		name:    name,
		mailbox: make(chan []byte),
	}
}

func (q *Queue) Run() {
	for msg := range q.mailbox {
		log.Printf("queue %s received: %q", q.name, msg)
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
