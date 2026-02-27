// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"testing"

	amqpclient "github.com/absmach/fluxmq/client/amqp"
	mqttclient "github.com/absmach/fluxmq/client/mqtt"
)

func TestResolveProtocolDefaultAndFallback(t *testing.T) {
	c := &Client{
		defaultProtocol: ProtocolAMQP,
		mqtt:            &mqttclient.Client{},
		amqp:            &amqpclient.Client{},
	}

	protocol, err := c.resolveProtocol("")
	if err != nil {
		t.Fatalf("resolveProtocol(\"\") error = %v", err)
	}
	if protocol != ProtocolAMQP {
		t.Fatalf("resolveProtocol(\"\") = %q, want %q", protocol, ProtocolAMQP)
	}

	c.defaultProtocol = ProtocolAMQP
	c.amqp = nil
	protocol, err = c.resolveProtocol("")
	if err != nil {
		t.Fatalf("resolveProtocol fallback error = %v", err)
	}
	if protocol != ProtocolMQTT {
		t.Fatalf("resolveProtocol fallback = %q, want %q", protocol, ProtocolMQTT)
	}
}

func TestResolveProtocolExplicit(t *testing.T) {
	c := &Client{
		defaultProtocol: ProtocolMQTT,
		mqtt:            &mqttclient.Client{},
		amqp:            &amqpclient.Client{},
	}

	protocol, err := c.resolveProtocol(ProtocolMQTT)
	if err != nil {
		t.Fatalf("resolveProtocol(ProtocolMQTT) error = %v", err)
	}
	if protocol != ProtocolMQTT {
		t.Fatalf("resolveProtocol(ProtocolMQTT) = %q, want %q", protocol, ProtocolMQTT)
	}

	protocol, err = c.resolveProtocol(ProtocolAMQP)
	if err != nil {
		t.Fatalf("resolveProtocol(ProtocolAMQP) error = %v", err)
	}
	if protocol != ProtocolAMQP {
		t.Fatalf("resolveProtocol(ProtocolAMQP) = %q, want %q", protocol, ProtocolAMQP)
	}
}

func TestResolveProtocolErrors(t *testing.T) {
	c := &Client{
		defaultProtocol: ProtocolMQTT,
		mqtt:            &mqttclient.Client{},
	}

	_, err := c.resolveProtocol(ProtocolAMQP)
	if !errors.Is(err, ErrNoRouteProtocol) {
		t.Fatalf("resolveProtocol(ProtocolAMQP) error = %v, want %v", err, ErrNoRouteProtocol)
	}

	_, err = c.resolveProtocol(Protocol("invalid"))
	if !errors.Is(err, ErrInvalidProtocol) {
		t.Fatalf("resolveProtocol(invalid) error = %v, want %v", err, ErrInvalidProtocol)
	}
}
