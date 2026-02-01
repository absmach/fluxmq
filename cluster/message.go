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
