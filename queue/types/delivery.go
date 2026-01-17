// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import "time"

// DeliveryState tracks inflight message delivery.
type DeliveryState struct {
	MessageID   string
	QueueName   string
	PartitionID int
	ConsumerID  string
	DeliveredAt time.Time
	Timeout     time.Time
	RetryCount  int
}
