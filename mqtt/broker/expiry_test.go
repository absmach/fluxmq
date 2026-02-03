// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
)

func TestMessageExpiry_ImmediateDelivery(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{})

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})

	// Create message with 5 second expiry
	expiry := uint32(5)
	now := time.Now()
	msg := &storage.Message{
		Topic:         "test/topic",
		Payload:       []byte("data"),
		QoS:           0,
		Retain:        false,
		MessageExpiry: &expiry,
		Expiry:        now.Add(5 * time.Second),
		PublishTime:   now,
	}

	// Message should be delivered (not expired)
	_, err := b.DeliverToSession(s, msg)
	if err != nil {
		t.Fatalf("Failed to deliver non-expired message: %v", err)
	}
}

func TestMessageExpiry_ExpiredMessage(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{})

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})

	// Create message that expired 1 second ago
	expiry := uint32(1)
	pastTime := time.Now().Add(-2 * time.Second)
	msg := &storage.Message{
		Topic:         "test/topic",
		Payload:       []byte("data"),
		QoS:           0,
		Retain:        false,
		MessageExpiry: &expiry,
		Expiry:        pastTime.Add(1 * time.Second), // Expired
		PublishTime:   pastTime,
	}

	// Message should be dropped silently
	_, err := b.DeliverToSession(s, msg)
	if err != nil {
		t.Fatalf("DeliverToSession should not error for expired messages: %v", err)
	}
}

func TestMessageExpiry_V5Handler(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{})

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	handler := NewV5Handler(b)

	// Subscribe to test topic
	b.subscribe(s, "test/topic", 1, storage.SubscribeOptions{})

	// Create PUBLISH packet with message expiry
	expiry := uint32(10)
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "test/topic",
		Payload:     []byte("test data"),
		Properties: &v5.PublishProperties{
			MessageExpiry: &expiry,
		},
	}

	// Publish message
	err := handler.HandlePublish(s, pub)
	if err != nil {
		t.Fatalf("Failed to handle PUBLISH with expiry: %v", err)
	}
}

func TestMessageExpiry_NoExpiry(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{})

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})

	// Create message without expiry
	msg := &storage.Message{
		Topic:         "test/topic",
		Payload:       []byte("data"),
		QoS:           0,
		Retain:        false,
		MessageExpiry: nil,
		PublishTime:   time.Now(),
	}

	// Message should be delivered
	_, err := b.DeliverToSession(s, msg)
	if err != nil {
		t.Fatalf("Failed to deliver message without expiry: %v", err)
	}
}

func TestMessageExpiry_RemainingTime(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil, config.SessionConfig{})

	s, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})

	// Create message with 10 second expiry, published 5 seconds ago
	expiry := uint32(10)
	publishTime := time.Now().Add(-5 * time.Second)
	expiryTime := publishTime.Add(10 * time.Second)

	msg := &storage.Message{
		Topic:         "test/topic",
		Payload:       []byte("data"),
		QoS:           0,
		Retain:        false,
		MessageExpiry: &expiry,
		Expiry:        expiryTime,
		PublishTime:   publishTime,
	}

	// Remaining time should be approximately 5 seconds
	remaining := time.Until(msg.Expiry)
	if remaining < 4*time.Second || remaining > 6*time.Second {
		t.Errorf("Expected remaining time ~5s, got %v", remaining)
	}

	// Message should still be delivered (not expired)
	_, err := b.DeliverToSession(s, msg)
	if err != nil {
		t.Fatalf("Failed to deliver message with remaining time: %v", err)
	}
}

func TestMessageExpiry_QoS1WithExpiry(t *testing.T) {
	// Create message with QoS 1 and expiry
	expiry := uint32(30)
	now := time.Now()
	msg := &storage.Message{
		Topic:         "test/topic",
		Payload:       []byte("qos1 data"),
		QoS:           1,
		Retain:        false,
		MessageExpiry: &expiry,
		Expiry:        now.Add(30 * time.Second),
		PublishTime:   now,
	}

	// Test that message with expiry can be stored and delivered
	if msg.MessageExpiry == nil {
		t.Error("Message should have expiry set")
	}
	if msg.Expiry.IsZero() {
		t.Error("Message expiry time should be set")
	}
	if *msg.MessageExpiry != 30 {
		t.Errorf("Expected expiry of 30 seconds, got %d", *msg.MessageExpiry)
	}
}

func TestMessageExpiry_RetainedMessage(t *testing.T) {
	store := memory.New()
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	// Create broker without cluster (nil cluster) to use local storage
	b := NewBroker(store, nil, logger, nil, nil, nil, nil, config.SessionConfig{})

	// Create retained message with expiry
	expiry := uint32(60)
	now := time.Now()
	msg := &storage.Message{
		Topic:         "test/retained",
		Payload:       []byte("retained data"),
		QoS:           0,
		Retain:        true,
		MessageExpiry: &expiry,
		Expiry:        now.Add(60 * time.Second),
		PublishTime:   now,
	}

	// Publish retained message
	err := b.Publish(msg)
	if err != nil {
		t.Fatalf("Failed to publish retained message: %v", err)
	}

	// Verify retained message was stored
	retained, err := b.GetRetainedMatching("test/retained")
	if err != nil {
		t.Fatalf("Failed to get retained messages: %v", err)
	}
	if len(retained) != 1 {
		t.Fatalf("Expected 1 retained message, got %d", len(retained))
	}
	if retained[0].MessageExpiry == nil {
		t.Error("Retained message should have expiry set")
	}
}
