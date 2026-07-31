// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"strings"
)

const (
	AMQP091ClientPrefix = "amqp091:"
	AMQP1ClientPrefix   = "amqp:"
	HTTPClientPrefix    = "http:"
	CoAPClientPrefix    = "coap:"
	ClientIDProperty    = "client_id"
	ExternalIDProperty  = "external_id"
	ProtocolProperty    = "protocol"
)

// ReservedPropertyPrefix marks message properties that carry broker-internal
// state between trusted services.
//
// Trust is a property of the connection's listener policy, never of its
// protocol. A connection carries reserved properties only when its policy
// marks it trusted. Every other connection has them stripped on ingress, so it
// cannot forge one, and on egress, so it cannot observe one another service
// set. MQTT, HTTP, CoAP, and AMQP 1.0 have no trusted listener and are
// therefore stripped in both directions; only the AMQP 0.9.1 internal
// listener is trusted, and only for ingress, because local principals are
// publish-only. A reserved property consequently reaches the broker from a
// trusted service today but is never handed back out to any connection.
const ReservedPropertyPrefix = "_flux."

// IsReservedProperty reports whether key names a broker-internal property.
func IsReservedProperty(key string) bool {
	return strings.HasPrefix(key, ReservedPropertyPrefix)
}

// Origin protocol identifiers written into ProtocolProperty by ingress
// handlers so downstream consumers can tell where a message came from.
const (
	ProtocolMQTT    = "mqtt"
	ProtocolAMQP091 = "amqp"
	ProtocolAMQP1   = "amqp1"
	ProtocolHTTP    = "http"
	ProtocolCoAP    = "coap"
)

// CrossDeliverFunc delivers a pub/sub message to a client in another protocol broker.
type CrossDeliverFunc func(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string)

// IsAMQP091Client returns true when the client ID belongs to an AMQP 0.9.1 connection.
func IsAMQP091Client(clientID string) bool {
	return strings.HasPrefix(clientID, AMQP091ClientPrefix)
}

// PrefixedAMQP091ClientID returns the canonical AMQP 0.9.1 client ID.
func PrefixedAMQP091ClientID(connID string) string {
	return AMQP091ClientPrefix + connID
}

// IsAMQP1Client returns true when the client ID belongs to an AMQP 1.0 connection.
func IsAMQP1Client(clientID string) bool {
	return strings.HasPrefix(clientID, AMQP1ClientPrefix)
}

// PrefixedAMQP1ClientID returns the canonical AMQP 1.0 client ID.
func PrefixedAMQP1ClientID(containerID string) string {
	return AMQP1ClientPrefix + containerID
}

// AddClientIDProperty writes the protocol-level client identity into the
// shared properties map used for cross-node and cross-protocol delivery.
// External identity must be set explicitly by the ingress handler.
func AddClientIDProperty(props map[string]string, clientID string) map[string]string {
	if clientID == "" {
		return props
	}
	if props == nil {
		props = make(map[string]string, 1)
	}
	props[ClientIDProperty] = clientID
	return props
}

// ClientIDFromProperties returns the protocol-level client identity carried in
// the shared properties map.
func ClientIDFromProperties(props map[string]string) string {
	if len(props) == 0 {
		return ""
	}
	return props[ClientIDProperty]
}

// ExternalIDFromProperties returns the authenticated external identity
// of the client that originally published the message.
func ExternalIDFromProperties(props map[string]string) string {
	if len(props) == 0 {
		return ""
	}
	return props[ExternalIDProperty]
}
