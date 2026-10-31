package main

import (
	"io"

	"github.com/go-stomp/stomp/v3/frame"
)

type StompCodec struct {
	reader *frame.Reader
	writer *frame.Writer
}

func NewStompCodec(rw io.ReadWriter) *StompCodec {
	return &StompCodec{
		reader: frame.NewReader(rw),
		writer: frame.NewWriter(rw),
	}
}

func (c *StompCodec) ReadFrame() (*frame.Frame, error) {
	for {
		f, err := c.reader.Read()
		if err != nil {
			return nil, err
		}
		if f != nil {
			return f, nil
		}
	}
}

func (c *StompCodec) WriteFrame(f *frame.Frame) error {
	return c.writer.Write(f)
}
