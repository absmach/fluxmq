// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"time"
)

// DeliveryMessage is the internal message format for queue delivery tracking.
type DeliveryMessage struct {
	ID          string
	Payload     []byte
	Topic       string
	Properties  map[string]string
	GroupID     string
	Offset      uint64
	DeliveredAt time.Time
	AckTopic    string
	NackTopic   string
	RejectTopic string
}

func extractGroupFromClientID(clientID string) string {
	for i, c := range clientID {
		if c == '-' {
			return clientID[:i]
		}
	}
	return clientID
}

// DefaultConsumerGroupID returns the queue-mode consumer group used when a
// subscriber does not provide one explicitly.
func DefaultConsumerGroupID(clientID string) string {
	return extractGroupFromClientID(clientID)
}
