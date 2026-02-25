// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"strconv"
	"sync/atomic"
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

var messageIDCounter atomic.Uint64

func generateMessageID() string {
	return strconv.FormatUint(messageIDCounter.Add(1), 10)
}

func parseMessageID(messageID string) (uint64, error) {
	// Format: queueName:offset (we only need the offset)
	for i := len(messageID) - 1; i >= 0; i-- {
		if messageID[i] == ':' {
			return strconv.ParseUint(messageID[i+1:], 10, 64)
		}
	}
	return strconv.ParseUint(messageID, 10, 64)
}
