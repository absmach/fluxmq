// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import "time"

// DeliveryState tracks inflight message delivery.
// Each consumer group has independent delivery tracking for fan-out support.
type DeliveryState struct {
	MessageID   string
	QueueName   string
	PartitionID int
	GroupID     string // Consumer group ID - required for fan-out to multiple groups
	ConsumerID  string
	DeliveredAt time.Time
	Timeout     time.Time
	RetryCount  int
}
