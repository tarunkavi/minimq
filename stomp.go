package main

import (
	"io"

	"github.com/go-stomp/stomp/v3/frame"
)

// StompCodec turns bytes from a connection into STOMP frames and back.
// The server only talks to the codec, so it never sees the wire format.
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

// ReadFrame deserializes the next frame. It blocks until a whole frame has
// arrived and skips heart-beats (empty lines), so it never returns a nil frame
// without an error.
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

// WriteFrame serializes f and sends it right away.
func (c *StompCodec) WriteFrame(f *frame.Frame) error {
	return c.writer.Write(f)
}
