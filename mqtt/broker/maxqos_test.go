// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"log/slog"
	"os"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
)

func TestMaxQoS_DefaultValue(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})

	if got := b.MaxQoS(); got != 2 {
		t.Errorf("Default MaxQoS() = %d, want 2", got)
	}
}

func TestMaxQoS_SetValue(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})

	tests := []struct {
		name    string
		setQoS  byte
		wantQoS byte
	}{
		{"set to 0", 0, 0},
		{"set to 1", 1, 1},
		{"set to 2", 2, 2},
		{"set to 3 (clamped to 2)", 3, 2},
		{"set to 255 (clamped to 2)", 255, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b.SetMaxQoS(tt.setQoS)
			if got := b.MaxQoS(); got != tt.wantQoS {
				t.Errorf("MaxQoS() = %d, want %d", got, tt.wantQoS)
			}
		})
	}
}

func TestMaxQoS_V5Handler_Downgrade_QoS2to0(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})

	// Set max QoS to 0 - any QoS 1 or 2 will be downgraded to 0
	b.SetMaxQoS(0)

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	handler := NewV5Handler(b)

	// Subscribe to capture the message
	b.subscribe(s, "test/topic", 2, storage.SubscribeOptions{})

	// Publish with QoS 2 (should be downgraded to 0, no ack needed)
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   "test/topic",
		Payload:     []byte("test data"),
		ID:          1,
	}

	// HandlePublish will downgrade the QoS to 0 before publishing
	// QoS 0 doesn't require acknowledgment so no connection needed
	err := handler.HandlePublish(s, pub)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}
}

func TestMaxQoS_V5Handler_Downgrade_QoS1to0(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})

	// Set max QoS to 0
	b.SetMaxQoS(0)

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	handler := NewV5Handler(b)

	// Subscribe to capture the message
	b.subscribe(s, "test/topic", 1, storage.SubscribeOptions{})

	// Publish with QoS 1 (should be downgraded to 0)
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "test/topic",
		Payload:     []byte("test data"),
		ID:          1,
	}

	err := handler.HandlePublish(s, pub)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}
}

func TestMaxQoS_V5Handler_NoDowngrade(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})

	// Set max QoS to 2 (no downgrade needed for QoS 0)
	b.SetMaxQoS(2)

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	handler := NewV5Handler(b)

	b.subscribe(s, "test/topic", 0, storage.SubscribeOptions{})

	// QoS 0 publish - no acknowledgment needed, no downgrade needed
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "test/topic",
		Payload:     []byte("test data"),
	}

	err := handler.HandlePublish(s, pub)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}
}

func TestMaxQoS_V3Handler_Downgrade(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})

	// Set max QoS to 0
	b.SetMaxQoS(0)

	s, _, _ := b.CreateSession("client1", 4, session.Options{CleanStart: true})
	handler := NewV3Handler(b)

	b.subscribe(s, "test/topic", 1, storage.SubscribeOptions{})

	// Publish with QoS 1 (should be downgraded to 0)
	pub := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "test/topic",
		Payload:     []byte("test data"),
		ID:          1,
	}

	err := handler.HandlePublish(s, pub)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}
}
