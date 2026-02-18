// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/storage"
)

func TestPublishCrossDeliverToAMQP091(t *testing.T) {
	b := NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	amqpClientID := corebroker.PrefixedAMQP091ClientID("conn-1")
	if err := b.router.Subscribe(amqpClientID, "telemetry/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	calls := 0
	var gotClientID string
	var gotTopic string
	var gotQoS byte
	b.SetCrossDeliver(func(clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		calls++
		gotClientID = clientID
		gotTopic = topic
		gotQoS = qos
	})

	msg := &storage.Message{
		Topic:      "telemetry/room1",
		QoS:        1,
		Properties: map[string]string{"source": "test"},
	}
	msg.SetPayloadFromBytes([]byte("hello"))

	if err := b.Publish(msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected 1 cross-deliver call, got %d", calls)
	}
	if gotClientID != amqpClientID {
		t.Fatalf("expected clientID %q, got %q", amqpClientID, gotClientID)
	}
	if gotTopic != "telemetry/room1" {
		t.Fatalf("expected topic telemetry/room1, got %q", gotTopic)
	}
	if gotQoS != 1 {
		t.Fatalf("expected QoS 1, got %d", gotQoS)
	}
}

func TestForwardPublishDoesNotCrossDeliverToAMQP091(t *testing.T) {
	b := NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	amqpClientID := corebroker.PrefixedAMQP091ClientID("conn-1")
	if err := b.router.Subscribe(amqpClientID, "telemetry/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	calls := 0
	b.SetCrossDeliver(func(clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		calls++
	})

	if err := b.ForwardPublish(context.Background(), &cluster.Message{
		Topic:      "telemetry/room1",
		Payload:    []byte("hello"),
		QoS:        1,
		Properties: map[string]string{"source": "test"},
	}); err != nil {
		t.Fatalf("ForwardPublish failed: %v", err)
	}

	if calls != 0 {
		t.Fatalf("expected 0 cross-deliver calls on ForwardPublish, got %d", calls)
	}
}
