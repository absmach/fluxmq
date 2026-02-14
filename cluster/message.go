// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

// Message is a lightweight wire format used for inter-broker communication.
// Contains only the essential publish fields needed for cross-node delivery.
type Message struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Dup        bool
	Properties map[string]string
}

// QueueMessage is a typed envelope for cross-node queue delivery.
// It separates queue metadata from user-defined message properties.
type QueueMessage struct {
	MessageID      string
	QueueName      string
	GroupID        string
	Payload        []byte
	Sequence       int64
	UserProperties map[string]string

	Stream          bool
	StreamOffset    int64
	StreamTimestamp int64 // Unix milliseconds

	HasWorkCommitted    bool
	WorkCommittedOffset int64
	WorkAcked           bool
	WorkGroup           string
}

// QueueDelivery pairs a queue message envelope with its target local client.
// Used for batched cross-node queue delivery.
type QueueDelivery struct {
	ClientID  string
	QueueName string
	Message   *QueueMessage
}
