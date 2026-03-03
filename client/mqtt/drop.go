// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import "time"

// DroppedMessageDirection identifies inbound or outbound drop path.
type DroppedMessageDirection string

const (
	DroppedMessageInbound  DroppedMessageDirection = "inbound"
	DroppedMessageOutbound DroppedMessageDirection = "outbound"
)

// DroppedMessageReason describes why the message was dropped.
type DroppedMessageReason string

const (
	DroppedReasonSlowConsumer         DroppedMessageReason = "slow_consumer"
	DroppedReasonOutboundBackpressure DroppedMessageReason = "outbound_backpressure"
	DroppedReasonReconnectBufferFull  DroppedMessageReason = "reconnect_buffer_full"
)

// DroppedMessage describes a message dropped by pressure policies.
type DroppedMessage struct {
	Direction   DroppedMessageDirection
	Reason      DroppedMessageReason
	Topic       string
	QoS         byte
	PayloadSize int
	Timestamp   time.Time
}
