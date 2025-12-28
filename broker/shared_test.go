// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"log/slog"
	"os"
	"testing"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/memory"
)

func TestSharedSubscription_GroupCreation(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	// Create 3 clients
	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})
	s3, _, _ := b.CreateSession("client3", 5, session.Options{CleanStart: true})

	// All 3 subscribe to the same shared subscription
	sharedFilter := "$share/group1/sensors/#"
	opts := storage.SubscribeOptions{}

	b.subscribe(s1, sharedFilter, 1, opts)
	b.subscribe(s2, sharedFilter, 1, opts)
	b.subscribe(s3, sharedFilter, 1, opts)

	// Verify share group was created with all 3 subscribers
	b.shareGroupsMu.RLock()
	group := b.shareGroups["group1/sensors/#"]
	b.shareGroupsMu.RUnlock()

	if group == nil {
		t.Fatal("Share group was not created")
	}
	if len(group.Subscribers) != 3 {
		t.Fatalf("Expected 3 subscribers, got %d", len(group.Subscribers))
	}
	if group.Name != "group1" {
		t.Errorf("Expected group name 'group1', got '%s'", group.Name)
	}
	if group.TopicFilter != "sensors/#" {
		t.Errorf("Expected topic filter 'sensors/#', got '%s'", group.TopicFilter)
	}
}

func TestSharedSubscription_RoundRobinSelection(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})
	s3, _, _ := b.CreateSession("client3", 5, session.Options{CleanStart: true})

	sharedFilter := "$share/workers/jobs/#"
	opts := storage.SubscribeOptions{}

	b.subscribe(s1, sharedFilter, 1, opts)
	b.subscribe(s2, sharedFilter, 1, opts)
	b.subscribe(s3, sharedFilter, 1, opts)

	b.shareGroupsMu.RLock()
	group := b.shareGroups["workers/jobs/#"]
	b.shareGroupsMu.RUnlock()

	// Test round-robin selection
	expected := []string{"client1", "client2", "client3", "client1", "client2", "client3"}
	for i, exp := range expected {
		selected := group.NextSubscriber()
		if selected != exp {
			t.Errorf("Round %d: expected '%s', got '%s'", i, exp, selected)
		}
	}
}

func TestSharedSubscription_Unsubscribe(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})

	sharedFilter := "$share/group1/test/topic"
	opts := storage.SubscribeOptions{}

	b.subscribe(s1, sharedFilter, 1, opts)
	b.subscribe(s2, sharedFilter, 1, opts)

	// Verify group exists
	b.shareGroupsMu.RLock()
	group := b.shareGroups["group1/test/topic"]
	b.shareGroupsMu.RUnlock()
	if group == nil {
		t.Fatal("Share group should exist")
	}
	if len(group.Subscribers) != 2 {
		t.Fatalf("Expected 2 subscribers, got %d", len(group.Subscribers))
	}

	// Unsubscribe client1
	b.unsubscribeInternal(s1, sharedFilter)

	// Group should still exist with 1 subscriber
	b.shareGroupsMu.RLock()
	group = b.shareGroups["group1/test/topic"]
	b.shareGroupsMu.RUnlock()
	if group == nil {
		t.Fatal("Share group should still exist")
	}
	if len(group.Subscribers) != 1 {
		t.Fatalf("Expected 1 subscriber, got %d", len(group.Subscribers))
	}

	// Unsubscribe client2
	b.unsubscribeInternal(s2, sharedFilter)

	// Group should be deleted
	b.shareGroupsMu.RLock()
	group = b.shareGroups["group1/test/topic"]
	b.shareGroupsMu.RUnlock()
	if group != nil {
		t.Fatal("Share group should be deleted when empty")
	}
}

func TestSharedSubscription_SessionDestroy(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})

	sharedFilter := "$share/mygroup/data/#"
	opts := storage.SubscribeOptions{}

	b.subscribe(s1, sharedFilter, 1, opts)
	b.subscribe(s2, sharedFilter, 1, opts)

	// Destroy client1's session
	b.DestroySession("client1")

	// Group should still exist with 1 subscriber
	b.shareGroupsMu.RLock()
	group := b.shareGroups["mygroup/data/#"]
	b.shareGroupsMu.RUnlock()
	if group == nil {
		t.Fatal("Share group should still exist")
	}
	if len(group.Subscribers) != 1 {
		t.Fatalf("Expected 1 subscriber after destroy, got %d", len(group.Subscribers))
	}
	if group.Subscribers[0] != "client2" {
		t.Errorf("Expected remaining subscriber to be client2, got %s", group.Subscribers[0])
	}

	// Destroy client2's session
	b.DestroySession("client2")

	// Group should be deleted
	b.shareGroupsMu.RLock()
	group = b.shareGroups["mygroup/data/#"]
	b.shareGroupsMu.RUnlock()
	if group != nil {
		t.Fatal("Share group should be deleted when all members are gone")
	}
}

func TestSharedSubscription_MultipleGroups(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})
	s3, _, _ := b.CreateSession("client3", 5, session.Options{CleanStart: true})

	opts := storage.SubscribeOptions{}

	// Two different share groups on the same topic
	b.subscribe(s1, "$share/group1/sensors/#", 1, opts)
	b.subscribe(s2, "$share/group2/sensors/#", 1, opts)

	// Client3 subscribes to both groups
	b.subscribe(s3, "$share/group1/sensors/#", 1, opts)

	// Both groups should exist
	b.shareGroupsMu.RLock()
	group1 := b.shareGroups["group1/sensors/#"]
	group2 := b.shareGroups["group2/sensors/#"]
	b.shareGroupsMu.RUnlock()

	if group1 == nil || group2 == nil {
		t.Fatal("Both share groups should exist")
	}
	if len(group1.Subscribers) != 2 {
		t.Fatalf("Group1 should have 2 subscribers, got %d", len(group1.Subscribers))
	}
	if len(group2.Subscribers) != 1 {
		t.Fatalf("Group2 should have 1 subscriber, got %d", len(group2.Subscribers))
	}
}

func TestSharedSubscription_SameGroupDifferentTopics(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})

	opts := storage.SubscribeOptions{}

	// Same share name but different topic filters
	b.subscribe(s1, "$share/workers/sensors/#", 1, opts)
	b.subscribe(s2, "$share/workers/logs/#", 1, opts)

	// Two separate groups should exist
	b.shareGroupsMu.RLock()
	sensorsGroup := b.shareGroups["workers/sensors/#"]
	logsGroup := b.shareGroups["workers/logs/#"]
	b.shareGroupsMu.RUnlock()

	if sensorsGroup == nil || logsGroup == nil {
		t.Fatal("Both share groups should exist")
	}
	if sensorsGroup.Name != "workers" || logsGroup.Name != "workers" {
		t.Error("Both groups should have the same share name")
	}
	if sensorsGroup.TopicFilter != "sensors/#" {
		t.Errorf("Expected sensors/#, got %s", sensorsGroup.TopicFilter)
	}
	if logsGroup.TopicFilter != "logs/#" {
		t.Errorf("Expected logs/#, got %s", logsGroup.TopicFilter)
	}
}

func TestSharedSubscription_DuplicateSubscribe(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})

	opts := storage.SubscribeOptions{}
	sharedFilter := "$share/group1/test/#"

	// Subscribe twice
	b.subscribe(s1, sharedFilter, 1, opts)
	b.subscribe(s1, sharedFilter, 1, opts)

	// Group should have the client only once
	b.shareGroupsMu.RLock()
	group := b.shareGroups["group1/test/#"]
	b.shareGroupsMu.RUnlock()

	if group == nil {
		t.Fatal("Share group should exist")
	}
	if len(group.Subscribers) != 1 {
		t.Fatalf("Expected 1 subscriber (no duplicates), got %d", len(group.Subscribers))
	}
}

func TestSharedSubscription_RouterIntegration(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test")
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(store, cl, logger, nil, nil, nil, nil)

	s1, _, _ := b.CreateSession("client1", 5, session.Options{CleanStart: true})
	s2, _, _ := b.CreateSession("client2", 5, session.Options{CleanStart: true})

	opts := storage.SubscribeOptions{}

	// Subscribe to shared subscription
	b.subscribe(s1, "$share/workers/tasks/#", 1, opts)
	b.subscribe(s2, "$share/workers/tasks/#", 1, opts)

	// Router should have the share group subscription, not individual clients
	matched, err := b.router.Match("tasks/job1")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Should have one match for the share group
	if len(matched) != 1 {
		t.Fatalf("Expected 1 match (share group), got %d", len(matched))
	}

	// The matched subscription should be for the share group
	if matched[0].ClientID != "$share/workers/tasks/#" {
		t.Errorf("Expected share group client ID, got '%s'", matched[0].ClientID)
	}
}
