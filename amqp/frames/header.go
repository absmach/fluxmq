// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package frames

import (
	"fmt"
	"io"
)

const (
	ProtoIDAMQP byte = 0x00
	ProtoIDSASL byte = 0x03

	ProtoHeaderSize = 8
)

// ProtocolHeader is the 8-byte AMQP protocol header.
// Format: "AMQP" + proto-id + major + minor + revision
var (
	AMQPHeader = [ProtoHeaderSize]byte{'A', 'M', 'Q', 'P', ProtoIDAMQP, 1, 0, 0}
	SASLHeader = [ProtoHeaderSize]byte{'A', 'M', 'Q', 'P', ProtoIDSASL, 1, 0, 0}
)

// WriteProtocolHeader writes the protocol header for the given protocol ID.
func WriteProtocolHeader(w io.Writer, protoID byte) error {
	var h [ProtoHeaderSize]byte
	copy(h[:4], "AMQP")
	h[4] = protoID
	h[5] = 1 // major
	h[6] = 0 // minor
	h[7] = 0 // revision
	_, err := w.Write(h[:])
	return err
}

// ReadProtocolHeader reads and validates an 8-byte protocol header.
// Returns the protocol ID.
func ReadProtocolHeader(r io.Reader) (byte, error) {
	var h [ProtoHeaderSize]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, err
	}

	if string(h[:4]) != "AMQP" {
		return 0, fmt.Errorf("invalid protocol header: expected AMQP, got %q", string(h[:4]))
	}

	if h[5] != 1 || h[6] != 0 || h[7] != 0 {
		return 0, fmt.Errorf("unsupported AMQP version %d.%d.%d", h[5], h[6], h[7])
	}

	return h[4], nil
}

// DetectAMQP checks if the first 4 bytes of the reader are "AMQP".
// This is used for protocol sniffing when multiplexing protocols on the same port.
func DetectAMQP(header []byte) bool {
	return len(header) >= 4 && string(header[:4]) == "AMQP"
}
