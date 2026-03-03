// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "time"

// Message represents a unified message across protocols.
type Message struct {
	Topic      string
	Payload    []byte
	Properties map[string]string
	Timestamp  time.Time

	Queue  string
	Offset uint64

	ackFn    func() error
	nackFn   func() error
	rejectFn func() error
}

// Ack acknowledges a queue message when supported.
func (m *Message) Ack() error {
	if m == nil || m.ackFn == nil {
		return nil
	}
	return m.ackFn()
}

// Nack negatively acknowledges a queue message when supported.
func (m *Message) Nack() error {
	if m == nil || m.nackFn == nil {
		return nil
	}
	return m.nackFn()
}

// Reject rejects a queue message when supported.
func (m *Message) Reject() error {
	if m == nil || m.rejectFn == nil {
		return nil
	}
	return m.rejectFn()
}

// MessageHandler handles an incoming unified message.
type MessageHandler func(msg *Message)
