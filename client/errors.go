// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "errors"

// Client errors.
var (
	// Configuration errors.
	ErrNoServers       = errors.New("no servers configured")
	ErrEmptyClientID   = errors.New("client ID cannot be empty")
	ErrInvalidProtocol = errors.New("invalid protocol version (must be 4 or 5)")

	// Connection errors.
	ErrNotConnected     = errors.New("client not connected")
	ErrAlreadyConnected = errors.New("client already connected")
	ErrConnectFailed    = errors.New("connection failed")
	ErrConnectRejected  = errors.New("connection rejected by broker")
	ErrConnectTimeout   = errors.New("connection timeout")

	// Operation errors.
	ErrTimeout              = errors.New("operation timed out")
	ErrMaxInflight          = errors.New("maximum inflight messages exceeded")
	ErrConnectionLost       = errors.New("connection lost")
	ErrClientClosed         = errors.New("client has been closed")
	ErrInvalidQoS           = errors.New("invalid QoS level (must be 0, 1, or 2)")
	ErrInvalidTopic         = errors.New("invalid topic")
	ErrSubscribeFailed      = errors.New("subscription failed")
	ErrQueueAckRequiresV5   = errors.New("queue acknowledgments require MQTT v5 user properties")
	ErrQueueAckMissingGroup = errors.New("group-id required for queue acknowledgment")

	// Protocol errors.
	ErrUnexpectedPacket = errors.New("unexpected packet type")
	ErrMalformedPacket  = errors.New("malformed packet")
)

// ConnAckCode represents MQTT CONNACK return codes.
type ConnAckCode byte

// MQTT 3.1.1 CONNACK return codes.
const (
	ConnAccepted           ConnAckCode = 0x00
	ConnRefusedProtocol    ConnAckCode = 0x01
	ConnRefusedIDRejected  ConnAckCode = 0x02
	ConnRefusedUnavailable ConnAckCode = 0x03
	ConnRefusedBadAuth     ConnAckCode = 0x04
	ConnRefusedNotAuth     ConnAckCode = 0x05
)

// String returns a human-readable description of the CONNACK code.
func (c ConnAckCode) String() string {
	switch c {
	case ConnAccepted:
		return "connection accepted"
	case ConnRefusedProtocol:
		return "unacceptable protocol version"
	case ConnRefusedIDRejected:
		return "client identifier rejected"
	case ConnRefusedUnavailable:
		return "server unavailable"
	case ConnRefusedBadAuth:
		return "bad username or password"
	case ConnRefusedNotAuth:
		return "not authorized"
	default:
		return "unknown error"
	}
}

// Error implements the error interface.
func (c ConnAckCode) Error() string {
	return c.String()
}
