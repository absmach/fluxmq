// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

// MessageStore provides persistence for QoS 1/2 messages.
// It stores outbound messages waiting for acknowledgment and
// inbound QoS 2 messages waiting for PUBREL.
type MessageStore interface {
	// StoreOutbound stores an outbound message awaiting acknowledgment.
	StoreOutbound(packetID uint16, msg *Message) error

	// GetOutbound retrieves an outbound message by packet ID.
	GetOutbound(packetID uint16) (*Message, bool)

	// DeleteOutbound removes an outbound message after acknowledgment.
	DeleteOutbound(packetID uint16) error

	// GetAllOutbound returns all stored outbound messages (for reconnection).
	GetAllOutbound() []*Message

	// StoreInbound stores an inbound QoS 2 message awaiting PUBREL.
	StoreInbound(packetID uint16, msg *Message) error

	// GetInbound retrieves an inbound QoS 2 message by packet ID.
	GetInbound(packetID uint16) (*Message, bool)

	// DeleteInbound removes an inbound message after PUBREL/PUBCOMP.
	DeleteInbound(packetID uint16) error

	// Reset clears all stored messages.
	Reset() error

	// Close releases any resources.
	Close() error
}
