// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
)

func TestMessageStore(t *testing.T) {
	s := NewMessageStore()

	// Test Store and Get
	msg := &storage.Message{
		Topic:    "test/topic",
		Payload:  []byte("hello"),
		QoS:      1,
		PacketID: 123,
	}

	if err := s.Store("client1/123", msg); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	got, err := s.Get("client1/123")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Topic != msg.Topic {
		t.Errorf("Topic mismatch: got %s, want %s", got.Topic, msg.Topic)
	}
	if string(got.Payload) != string(msg.Payload) {
		t.Errorf("Payload mismatch: got %s, want %s", got.Payload, msg.Payload)
	}

	// Test mutation isolation
	msg.Payload[0] = 'x'
	got2, _ := s.Get("client1/123")
	if string(got2.Payload) != "hello" {
		t.Errorf("Mutation affected stored message")
	}

	// Test List with prefix
	s.Store("client1/456", &storage.Message{Topic: "t2"})
	s.Store("client2/789", &storage.Message{Topic: "t3"})

	list, err := s.List("client1/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("List returned %d messages, want 2", len(list))
	}

	// Test Delete
	if err := s.Delete("client1/123"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = s.Get("client1/123")
	if err != storage.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}

	// Test DeleteByPrefix
	if err := s.DeleteByPrefix("client1/"); err != nil {
		t.Fatalf("DeleteByPrefix failed: %v", err)
	}
	list, _ = s.List("client1/")
	if len(list) != 0 {
		t.Errorf("Expected 0 messages after DeleteByPrefix, got %d", len(list))
	}
}

func TestSubscriptionStore(t *testing.T) {
	s := NewSubscriptionStore()

	// Test Add
	sub1 := &storage.Subscription{
		ClientID: "client1",
		Filter:   "home/+/temp",
		QoS:      1,
	}
	if err := s.Add(sub1); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	sub2 := &storage.Subscription{
		ClientID: "client2",
		Filter:   "home/#",
		QoS:      2,
	}
	if err := s.Add(sub2); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Test Count
	if s.Count() != 2 {
		t.Errorf("Count: got %d, want 2", s.Count())
	}

	// Test Match with exact
	matched, err := s.Match("home/bedroom/temp")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}
	if len(matched) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matched))
	}

	// Test Match - only wildcard
	matched, err = s.Match("home/bedroom/humidity")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}
	if len(matched) != 1 {
		t.Errorf("Expected 1 match (home/#), got %d", len(matched))
	}

	// Test GetForClient
	subs, err := s.GetForClient("client1")
	if err != nil {
		t.Fatalf("GetForClient failed: %v", err)
	}
	if len(subs) != 1 {
		t.Errorf("Expected 1 subscription for client1, got %d", len(subs))
	}

	// Test Remove
	if err := s.Remove("client1", "home/+/temp"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if s.Count() != 1 {
		t.Errorf("Count after remove: got %d, want 1", s.Count())
	}

	// Test RemoveAll
	s.Add(&storage.Subscription{ClientID: "client2", Filter: "other/topic", QoS: 0})
	if err := s.RemoveAll("client2"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}
	if s.Count() != 0 {
		t.Errorf("Count after RemoveAll: got %d, want 0", s.Count())
	}
}

func TestSubscriptionStoreDeduplication(t *testing.T) {
	s := NewSubscriptionStore()

	// Add overlapping subscriptions for same client
	s.Add(&storage.Subscription{ClientID: "client1", Filter: "home/#", QoS: 1})
	s.Add(&storage.Subscription{ClientID: "client1", Filter: "home/+/temp", QoS: 2})

	matched, err := s.Match("home/bedroom/temp")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Should only get one result per client (highest QoS)
	if len(matched) != 1 {
		t.Errorf("Expected 1 deduplicated match, got %d", len(matched))
	}
	if matched[0].QoS != 2 {
		t.Errorf("Expected highest QoS (2), got %d", matched[0].QoS)
	}
}

func TestRetainedStore(t *testing.T) {
	s := NewRetainedStore()
	ctx := context.Background()

	// Test Set and Get
	msg := &storage.Message{
		Topic:   "sensors/temp",
		Payload: []byte("23.5"),
		QoS:     1,
		Retain:  true,
	}

	if err := s.Set(ctx, "sensors/temp", msg); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := s.Get(ctx, "sensors/temp")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got.Payload) != "23.5" {
		t.Errorf("Payload mismatch")
	}

	// Test Match with exact filter
	s.Set(ctx, "sensors/humidity", &storage.Message{Payload: []byte("60")})
	s.Set(ctx, "sensors/pressure", &storage.Message{Payload: []byte("1013")})

	matched, err := s.Match(ctx, "sensors/+")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}
	if len(matched) != 3 {
		t.Errorf("Expected 3 matches, got %d", len(matched))
	}

	// Test Match with #
	matched, err = s.Match(ctx, "sensors/#")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}
	if len(matched) != 3 {
		t.Errorf("Expected 3 matches with #, got %d", len(matched))
	}

	// Test Delete via empty payload
	if err := s.Set(ctx, "sensors/temp", &storage.Message{Payload: nil}); err != nil {
		t.Fatalf("Set with empty payload failed: %v", err)
	}
	_, err = s.Get(ctx, "sensors/temp")
	if err != storage.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}

	// Test Delete
	if err := s.Delete(ctx, "sensors/humidity"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	matched, _ = s.Match(ctx, "sensors/#")
	if len(matched) != 1 {
		t.Errorf("Expected 1 match after delete, got %d", len(matched))
	}
}

func TestRetainedStoreSystemTopics(t *testing.T) {
	s := NewRetainedStore()
	ctx := context.Background()

	s.Set(ctx, "$SYS/broker/clients", &storage.Message{Payload: []byte("10")})
	s.Set(ctx, "normal/topic", &storage.Message{Payload: []byte("data")})

	// # should not match $SYS topics
	matched, _ := s.Match(ctx, "#")
	if len(matched) != 1 {
		t.Errorf("# should not match $SYS topics, got %d matches", len(matched))
	}

	// + should not match $SYS topics
	matched, _ = s.Match(ctx, "+/broker/clients")
	if len(matched) != 0 {
		t.Errorf("+ should not match $SYS topics, got %d matches", len(matched))
	}

	// Explicit $SYS filter should match
	matched, _ = s.Match(ctx, "$SYS/#")
	if len(matched) != 1 {
		t.Errorf("$SYS/# should match $SYS topics, got %d matches", len(matched))
	}
}

func TestWillStore(t *testing.T) {
	s := NewWillStore()
	ctx := context.Background()

	// Test Set and Get
	will := &storage.WillMessage{
		ClientID: "client1",
		Topic:    "clients/client1/status",
		Payload:  []byte("offline"),
		QoS:      1,
		Retain:   true,
		Delay:    5, // 5 second delay
	}

	if err := s.Set(ctx, "client1", will); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := s.Get(ctx, "client1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Topic != will.Topic {
		t.Errorf("Topic mismatch")
	}
	if got.Delay != will.Delay {
		t.Errorf("Delay mismatch")
	}

	// Test GetPending - should be empty (not disconnected yet)
	pending, err := s.GetPending(ctx, time.Now())
	if err != nil {
		t.Fatalf("GetPending failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("Expected 0 pending wills before disconnect, got %d", len(pending))
	}

	// Mark as disconnected
	s.MarkDisconnected("client1")

	// Still not pending (delay not elapsed)
	pending, _ = s.GetPending(ctx, time.Now())
	if len(pending) != 0 {
		t.Errorf("Expected 0 pending wills before delay, got %d", len(pending))
	}

	// After delay elapsed
	pending, _ = s.GetPending(ctx, time.Now().Add(10*time.Second))
	if len(pending) != 1 {
		t.Errorf("Expected 1 pending will after delay, got %d", len(pending))
	}

	// Test Delete
	if err := s.Delete(ctx, "client1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = s.Get(ctx, "client1")
	if err != storage.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete")
	}
}

func TestCompositeStore(t *testing.T) {
	s := New()

	// Test all sub-stores are accessible
	if s.Messages() == nil {
		t.Error("Messages() returned nil")
	}
	if s.Sessions() == nil {
		t.Error("Sessions() returned nil")
	}
	if s.Subscriptions() == nil {
		t.Error("Subscriptions() returned nil")
	}
	if s.Retained() == nil {
		t.Error("Retained() returned nil")
	}
	if s.Wills() == nil {
		t.Error("Wills() returned nil")
	}

	// Test Close
	if err := s.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
