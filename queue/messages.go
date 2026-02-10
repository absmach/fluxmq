// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
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

func generateMessageID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func parseMessageID(messageID string) (uint64, error) {
	var offset uint64
	// Format: queueName:offset (we only need the offset)
	for i := len(messageID) - 1; i >= 0; i-- {
		if messageID[i] == ':' {
			_, err := fmt.Sscanf(messageID[i+1:], "%d", &offset)
			return offset, err
		}
	}
	// Try parsing as just an offset
	_, err := fmt.Sscanf(messageID, "%d", &offset)
	return offset, err
}
