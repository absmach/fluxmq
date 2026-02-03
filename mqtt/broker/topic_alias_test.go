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
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
}

func TestTopicAlias_RegisterAndResolve(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	b := NewBroker(store, cl, testLogger(), nil, nil, nil, nil, config.SessionConfig{})

	// Create session with TopicAliasMax = 10
	s, _, err := b.CreateSession("test-client", 5, session.Options{
		CleanStart: true,
	})
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	s.TopicAliasMax = 10

	handler := NewV5Handler(b)

	// Test 1: Register alias with first PUBLISH (topic + alias)
	alias1 := uint16(1)
	pub1 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "test/topic1",
		Payload:     []byte("hello"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias1,
		},
	}

	if err := handler.HandlePublish(s, pub1); err != nil {
		t.Fatalf("Failed to handle PUBLISH with alias registration: %v", err)
	}

	// Verify alias was registered
	topic, ok := s.ResolveInboundAlias(1)
	if !ok {
		t.Fatal("Alias 1 was not registered")
	}
	if topic != "test/topic1" {
		t.Fatalf("Expected topic 'test/topic1', got '%s'", topic)
	}

	// Test 2: Use alias without topic (should resolve)
	pub2 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "", // Empty - should be resolved via alias
		Payload:     []byte("world"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias1,
		},
	}

	if err := handler.HandlePublish(s, pub2); err != nil {
		t.Fatalf("Failed to handle PUBLISH with alias resolution: %v", err)
	}

	// Test 3: Exceed TopicAliasMax (should fail)
	alias11 := uint16(11)
	pub3 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "test/topic2",
		Payload:     []byte("fail"),
		ID:          1,
		Properties: &v5.PublishProperties{
			TopicAlias: &alias11,
		},
	}

	err = handler.HandlePublish(s, pub3)
	if err == nil {
		t.Fatal("Expected error when alias exceeds TopicAliasMax, got nil")
	}

	// Test 4: Use unregistered alias (should fail)
	alias5 := uint16(5)
	pub4 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "", // Empty with unregistered alias
		Payload:     []byte("fail"),
		ID:          2,
		Properties: &v5.PublishProperties{
			TopicAlias: &alias5,
		},
	}

	err = handler.HandlePublish(s, pub4)
	if err == nil {
		t.Fatal("Expected error when using unregistered alias, got nil")
	}
}

func TestTopicAlias_MultipleAliases(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	b := NewBroker(store, cl, testLogger(), nil, nil, nil, nil, config.SessionConfig{})

	s, _, err := b.CreateSession("test-client", 5, session.Options{
		CleanStart: true,
	})
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	s.TopicAliasMax = 100

	handler := NewV5Handler(b)

	// Register multiple aliases
	aliases := map[uint16]string{
		1:  "home/temperature",
		2:  "home/humidity",
		10: "sensors/data",
	}

	for alias, topic := range aliases {
		aliasVal := alias
		pub := &v5.Publish{
			FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
			TopicName:   topic,
			Payload:     []byte("data"),
			Properties: &v5.PublishProperties{
				TopicAlias: &aliasVal,
			},
		}

		if err := handler.HandlePublish(s, pub); err != nil {
			t.Fatalf("Failed to register alias %d: %v", alias, err)
		}
	}

	// Verify all aliases are registered
	for alias, expectedTopic := range aliases {
		topic, ok := s.ResolveInboundAlias(alias)
		if !ok {
			t.Fatalf("Alias %d was not registered", alias)
		}
		if topic != expectedTopic {
			t.Fatalf("Alias %d: expected topic '%s', got '%s'", alias, expectedTopic, topic)
		}
	}

	// Use aliases without topics
	for alias := range aliases {
		aliasVal := alias
		pub := &v5.Publish{
			FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
			TopicName:   "",
			Payload:     []byte("using-alias"),
			Properties: &v5.PublishProperties{
				TopicAlias: &aliasVal,
			},
		}

		if err := handler.HandlePublish(s, pub); err != nil {
			t.Fatalf("Failed to use alias %d: %v", alias, err)
		}
	}
}

func TestTopicAlias_UpdateExisting(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	b := NewBroker(store, cl, testLogger(), nil, nil, nil, nil, config.SessionConfig{})

	s, _, err := b.CreateSession("test-client", 5, session.Options{
		CleanStart: true,
	})
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	s.TopicAliasMax = 10

	handler := NewV5Handler(b)

	// Register alias 1 with topic A
	alias := uint16(1)
	pub1 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "topic/a",
		Payload:     []byte("data"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias,
		},
	}

	if err := handler.HandlePublish(s, pub1); err != nil {
		t.Fatalf("Failed to register alias: %v", err)
	}

	// Update alias 1 with topic B
	pub2 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "topic/b",
		Payload:     []byte("data"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias,
		},
	}

	if err := handler.HandlePublish(s, pub2); err != nil {
		t.Fatalf("Failed to update alias: %v", err)
	}

	// Verify alias now resolves to topic B
	topic, ok := s.ResolveInboundAlias(1)
	if !ok {
		t.Fatal("Alias 1 was not registered")
	}
	if topic != "topic/b" {
		t.Fatalf("Expected alias to be updated to 'topic/b', got '%s'", topic)
	}
}

func TestTopicAlias_SessionIsolation(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	b := NewBroker(store, cl, testLogger(), nil, nil, nil, nil, config.SessionConfig{})

	// Create two sessions
	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s1.TopicAliasMax = 10

	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})
	s2.TopicAliasMax = 10

	handler := NewV5Handler(b)

	// Register alias 1 in session 1
	alias := uint16(1)
	pub1 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "session1/topic",
		Payload:     []byte("data"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias,
		},
	}
	handler.HandlePublish(s1, pub1)

	// Register alias 1 in session 2 with different topic
	pub2 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "session2/topic",
		Payload:     []byte("data"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias,
		},
	}
	handler.HandlePublish(s2, pub2)

	// Verify aliases are isolated
	topic1, _ := s1.ResolveInboundAlias(1)
	topic2, _ := s2.ResolveInboundAlias(1)

	if topic1 != "session1/topic" {
		t.Fatalf("Session 1 alias incorrect: got '%s'", topic1)
	}
	if topic2 != "session2/topic" {
		t.Fatalf("Session 2 alias incorrect: got '%s'", topic2)
	}
}
