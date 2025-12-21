// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package core

// Message represents an MQTT PUBLISH message for routing between brokers.
// This is a lightweight wire format used for inter-broker communication in the cluster.
//
// It is used for routing messages between broker nodes in a cluster.
// Contains only the essential MQTT PUBLISH fields needed for delivery.
// Does NOT include persistence-related fields (timestamps, packet IDs, etc.).

type Message struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Dup        bool
	Properties map[string]string
}
