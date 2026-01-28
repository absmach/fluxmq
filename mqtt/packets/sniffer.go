// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package packets

import (
	"bytes"
	"errors"
	"io"
)

// ErrInvalidProtocol indicates the protocol name or structure is invalid for MQTT.
var ErrInvalidProtocol = errors.New("invalid protocol")

// DetectProtocolVersion peeks at the first few bytes of the connection to determine
// the MQTT protocol version without consuming the bytes permanently.
// It returns the protocol version (e.g. 4 for 3.1.1, 5 for 5.0), a reader that
// allows re-reading the bytes, and any error encountered.
func DetectProtocolVersion(r io.Reader) (int, io.Reader, error) {
	// A Connect packet is at least:
	// Fixed Header: 2 bytes (Type + Remaining Length)
	// Protocol Name Length: 2 bytes
	// Protocol Name: 4 bytes ("MQTT") or 6 bytes ("MQIsdp")
	// Protocol Version: 1 byte
	// Total minimal peek: ~10 bytes to be safe.

	peekBuf := make([]byte, 12)
	n, err := io.ReadFull(r, peekBuf)
	// If we hit EOF but read some bytes, we might have a short packet, but we can still check what we got.
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, nil, err
	}
	if n < 8 {
		// Not enough bytes to be a valid CONNECT packet
		return 0, bytes.NewReader(peekBuf[:n]), nil
	}

	// Create a reader to restore the consumed bytes
	restoredReader := io.MultiReader(bytes.NewReader(peekBuf[:n]), r)

	// Check Fixed Header (Connect Packet Type is 0x10)
	if peekBuf[0]&0xF0 != 0x10 {
		return 0, restoredReader, nil // Not a CONNECT packet, detection failed but stream preserved
	}

	// Simple check for "MQTT" (V3.1.1 or V5) or "MQIsdp" (V3.1)
	// The variable header starts after remaining length.
	// Remaining Length can be 1-4 bytes. We need to skip it.
	idx := 1
	multiplier := 1
	var remainingLength int
	for i := 0; i < 4; i++ {
		if idx >= n {
			return 0, restoredReader, nil
		}
		digit := peekBuf[idx]
		idx++
		remainingLength += int(digit&0x7F) * multiplier
		if digit&0x80 == 0 {
			break
		}
		multiplier *= 128
	}

	if idx+6 > n {
		return 0, restoredReader, nil // Not enough bytes for Variable Header
	}

	// Protocol Name Length (Big Endian uint16)
	protoLen := int(peekBuf[idx])<<8 | int(peekBuf[idx+1])
	idx += 2

	if idx+protoLen+1 > n {
		return 0, restoredReader, nil
	}

	protoName := string(peekBuf[idx : idx+protoLen])
	idx += protoLen

	version := int(peekBuf[idx])

	// Validate minimal consistency
	if protoName == "MQTT" {
		if version == 4 || version == 5 {
			return version, restoredReader, nil
		}
	} else if protoName == "MQIsdp" && version == 3 {
		return version, restoredReader, nil
	}

	return 0, restoredReader, ErrInvalidProtocol
}

// DetectPacketType peeks at the first byte of the stream to determine the packet type.
// It returns the packet type (e.g. 1 for CONNECT, 3 for PUBLISH), a reader that
// allows re-reading the byte, and any error encountered.
func DetectPacketType(r io.Reader) (byte, io.Reader, error) {
	peekBuf := make([]byte, 1)
	n, err := io.ReadFull(r, peekBuf)
	if err != nil && err != io.EOF {
		return 0, nil, err
	}
	if n == 0 {
		return 0, r, nil // Empty stream
	}

	restoredReader := io.MultiReader(bytes.NewReader(peekBuf[:n]), r)
	packetType := peekBuf[0] >> 4
	return packetType, restoredReader, nil
}
