// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package frames

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// Frame types
	FrameTypeAMQP byte = 0x00
	FrameTypeSASL byte = 0x01

	// Minimum frame size per AMQP 1.0 spec
	MinFrameSize uint32 = 512

	// Default max frame size
	DefaultMaxFrameSize uint32 = 65536

	// Frame header size: 4 (size) + 1 (doff) + 1 (type) + 2 (channel)
	HeaderSize = 8

	// Minimum data offset (in 4-byte words) = 2 (8 bytes header)
	MinDOFF = 2
)

// Frame represents an AMQP 1.0 frame.
type Frame struct {
	Type    byte
	Channel uint16
	Body    []byte
}

// WriteFrame writes an AMQP frame to the writer.
// maxFrameSize of 0 means no limit.
func WriteFrame(w io.Writer, frameType byte, channel uint16, body []byte) error {
	size := uint32(HeaderSize + len(body))

	var header [HeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], size)
	header[4] = MinDOFF // DOFF = 2 (8 bytes / 4)
	header[5] = frameType
	binary.BigEndian.PutUint16(header[6:8], channel)

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

// ReadFrame reads a single AMQP frame from the reader.
func ReadFrame(r io.Reader) (*Frame, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header[0:4])
	doff := header[4]
	frameType := header[5]
	channel := binary.BigEndian.Uint16(header[6:8])

	if size < HeaderSize {
		return nil, fmt.Errorf("frame size %d is less than minimum %d", size, HeaderSize)
	}

	// DOFF is in 4-byte words, skip any extended header
	extHeaderSize := int(doff)*4 - HeaderSize
	if extHeaderSize < 0 {
		return nil, fmt.Errorf("invalid DOFF value: %d", doff)
	}
	if extHeaderSize > 0 {
		if _, err := io.ReadFull(r, make([]byte, extHeaderSize)); err != nil {
			return nil, err
		}
	}

	bodySize := int(size) - int(doff)*4
	if bodySize < 0 {
		return nil, fmt.Errorf("invalid frame: body size is negative")
	}

	var body []byte
	if bodySize > 0 {
		body = make([]byte, bodySize)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
	}

	return &Frame{
		Type:    frameType,
		Channel: channel,
		Body:    body,
	}, nil
}

// WriteEmptyFrame writes a heartbeat (empty) frame.
func WriteEmptyFrame(w io.Writer) error {
	return WriteFrame(w, FrameTypeAMQP, 0, nil)
}

// IsEmpty returns true if the frame has no body (heartbeat).
func (f *Frame) IsEmpty() bool {
	return len(f.Body) == 0
}
