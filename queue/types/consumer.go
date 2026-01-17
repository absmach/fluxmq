// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import "time"

// Consumer represents a queue consumer.
type Consumer struct {
	ID            string
	ClientID      string
	GroupID       string
	QueueName     string
	AssignedParts []int
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	ProxyNodeID   string // For cluster routing
}

// ConsumerGroup represents a group of consumers.
type ConsumerGroup struct {
	ID        string
	QueueName string
	Consumers map[string]*Consumer
}
