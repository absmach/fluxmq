// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

const (
	// Queue delivery metadata properties.
	PropMessageID = "message-id"
	PropGroupID   = "group-id"
	PropQueueName = "queue"
	PropOffset    = "offset"

	// Stream delivery metadata properties.
	PropStreamOffset    = "x-stream-offset"
	PropStreamTimestamp = "x-stream-timestamp"

	// Work stealing metadata properties.
	PropWorkCommittedOffset = "x-work-committed-offset"
	PropWorkAcked           = "x-work-acked"
	PropWorkGroup           = "x-work-group"

	// Queue commit headers/properties.
	PropCommitGroupID = "x-group-id"
	PropCommitOffset  = "x-offset"

	// Queue reject metadata.
	PropRejectReason = "reason"
)

// IsReservedQueueDeliveryProperty returns true for keys managed by queue routing.
func IsReservedQueueDeliveryProperty(key string) bool {
	switch key {
	case PropMessageID, PropGroupID, PropQueueName, PropOffset,
		PropStreamOffset, PropStreamTimestamp,
		PropWorkCommittedOffset, PropWorkAcked, PropWorkGroup:
		return true
	default:
		return false
	}
}
