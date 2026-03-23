// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import "errors"

// Client errors.
var (
	// Configuration errors.
	ErrNoServers       = errors.New("no servers configured")
	ErrEmptyClientID   = errors.New("client ID cannot be empty")
	ErrInvalidProtocol = errors.New("invalid protocol version (must be 4 or 5)")
	ErrNilOptions      = errors.New("options cannot be nil")

	// Connection errors.
	ErrNotConnected     = errors.New("client not connected")
	ErrAlreadyConnected = errors.New("client already connected")
	ErrConnectFailed    = errors.New("connection failed")
	ErrConnectRejected  = errors.New("connection rejected by broker")
	ErrConnectTimeout   = errors.New("connection timeout")
	ErrPingTimeout      = errors.New("ping response timeout")

	// Operation errors.
	ErrTimeout              = errors.New("operation timed out")
	ErrMaxInflight          = errors.New("maximum inflight messages exceeded")
	ErrConnectionLost       = errors.New("connection lost")
	ErrClientClosed         = errors.New("client has been closed")
	ErrDraining             = errors.New("client is draining")
	ErrInvalidMessage       = errors.New("invalid message")
	ErrInvalidSubscribeOpt  = errors.New("invalid subscribe option")
	ErrInvalidQoS           = errors.New("invalid QoS level (must be 0, 1, or 2)")
	ErrInvalidTopic         = errors.New("invalid topic")
	ErrSubscribeFailed      = errors.New("subscription failed")
	ErrSlowConsumer         = errors.New("slow consumer: message dropped")
	ErrOutboundBackpressure = errors.New("outbound publish backpressure: message dropped")
	ErrReconnectBufferFull  = errors.New("reconnect publish buffer is full")
	ErrQueueAckRequiresV5   = errors.New("queue acknowledgments require MQTT v5 user properties")
	ErrQueueAckMissingGroup = errors.New("group-id required for queue acknowledgment")

	// Authentication errors.
	ErrAuthFailed         = errors.New("enhanced authentication failed")
	ErrNoAuthHandler      = errors.New("server sent AUTH but no OnAuth handler configured")
	ErrAuthNotV5          = errors.New("enhanced authentication requires MQTT 5.0")
	ErrAuthMethodMissing  = errors.New("enhanced authentication method not set")
	ErrAuthMethodMismatch = errors.New("enhanced authentication method mismatch")

	// Protocol errors.
	ErrUnexpectedPacket = errors.New("unexpected packet type")
	ErrMalformedPacket  = errors.New("malformed packet")
)

// ConnAckError represents MQTT CONNACK return codes.
type ConnAckError byte

// MQTT 3.1.1 CONNACK return codes.
const (
	ErrConnAccepted           ConnAckError = 0x00
	ErrConnRefusedProtocol    ConnAckError = 0x01
	ErrConnRefusedIDRejected  ConnAckError = 0x02
	ErrConnRefusedUnavailable ConnAckError = 0x03
	ErrConnRefusedBadAuth     ConnAckError = 0x04
	ErrConnRefusedNotAuth     ConnAckError = 0x05
)

// String returns a human-readable description of the CONNACK code.
func (c ConnAckError) String() string {
	switch c {
	case ErrConnAccepted:
		return "connection accepted"
	case ErrConnRefusedProtocol:
		return "unacceptable protocol version"
	case ErrConnRefusedIDRejected:
		return "client identifier rejected"
	case ErrConnRefusedUnavailable:
		return "server unavailable"
	case ErrConnRefusedBadAuth:
		return "bad username or password"
	case ErrConnRefusedNotAuth:
		return "not authorized"
	default:
		return "unknown error"
	}
}

// Error implements the error interface.
func (c ConnAckError) Error() string {
	return c.String()
}
