// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

//go:build ignore

// TODO: enable when queue/storage/badger packages are implemented

package queue

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	queueLogBadger "github.com/absmach/fluxmq/queue/storage/badger/log"
	brokerstorage "github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
)

func TestWildcardQueueSubscriptionBadger(t *testing.T) {
	// Create temp directory for BadgerDB
	tmpDir, err := os.MkdirTemp("", "queue-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Open BadgerDB
	opts := badger.DefaultOptions(tmpDir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	// Create stores
	logStore := queueLogBadger.New(db)

	deliveredMsgs := make(chan *brokerstorage.Message, 10)

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		if brokerMsg, ok := msg.(*brokerstorage.Message); ok {
			t.Logf("Delivered message to %s: topic=%s", clientID, brokerMsg.Topic)
			deliveredMsgs <- brokerMsg
		} else {
			t.Errorf("Wrong message type: %T", msg)
		}
		return nil
	}

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	// Note: logStore implements both LogStore and ConsumerGroupStore
	manager := NewManager(logStore, logStore, deliverFn, config, logger)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe to wildcard pattern
	clientID := "test-client-1"
	filter := "$queue/topic/#"

	t.Logf("Subscribing client %s to %s", clientID, filter)
	if err := manager.Subscribe(ctx, filter, clientID, "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Check that queue was created
	queues, _ := logStore.ListQueues(ctx)
	t.Logf("Queues after subscribe: %v", len(queues))
	for _, q := range queues {
		t.Logf("  Queue: %s (partitions=%d)", q.Name, q.Partitions)
	}

	// Check that group was created
	groups, _ := logStore.ListConsumerGroups(ctx, "$queue/topic")
	t.Logf("Groups after subscribe: %v", len(groups))
	for _, g := range groups {
		t.Logf("  Group: %s (pattern=%s, consumers=%d)", g.ID, g.Pattern, len(g.Consumers))
		for cid, ci := range g.Consumers {
			t.Logf("    Consumer: %s (clientID=%s)", cid, ci.ClientID)
		}
	}

	// Publish a message
	publishTopic := "$queue/topic/test"
	payload := []byte("hello world")

	t.Logf("Publishing message to %s", publishTopic)
	if err := manager.Enqueue(ctx, publishTopic, payload, nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Check message was appended
	tail, _ := logStore.Tail(ctx, "$queue/topic", 0)
	t.Logf("Tail after publish: %d", tail)

	// Wait for delivery
	t.Log("Waiting for message delivery...")
	select {
	case msg := <-deliveredMsgs:
		t.Logf("Received message: topic=%s payload=%s", msg.Topic, string(msg.GetPayload()))
		if msg.Topic != publishTopic {
			t.Errorf("Expected topic %s, got %s", publishTopic, msg.Topic)
		}
		if string(msg.GetPayload()) != string(payload) {
			t.Errorf("Expected payload %s, got %s", payload, msg.GetPayload())
		}
	case <-time.After(2 * time.Second):
		// Debug: check state
		groups, _ = logStore.ListConsumerGroups(ctx, "$queue/topic")
		for _, g := range groups {
			t.Logf("Group state: %s cursor=%v", g.ID, g.Cursors)
		}
		t.Fatal("Timeout waiting for message delivery")
	}
}
