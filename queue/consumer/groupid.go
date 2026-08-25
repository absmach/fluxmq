// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

// DefaultConsumerGroupID returns the queue-mode consumer group used when a
// subscriber does not provide one explicitly.
//
// The convention is that a client identifies its group by the portion of its
// client ID before the first hyphen, so several instances of one application
// join the same group without coordinating.
func DefaultConsumerGroupID(clientID string) string {
	for i, c := range clientID {
		if c == '-' {
			return clientID[:i]
		}
	}
	return clientID
}
