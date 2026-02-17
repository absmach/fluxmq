// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/storage"
)

func TestGetSubscribersForTopicUsesTrie(t *testing.T) {
	c := &EtcdCluster{
		subTrie: router.NewRouter(),
	}

	if err := c.subTrie.Subscribe("c1", "sensors/+/temp", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := c.subTrie.Subscribe("c2", "alerts/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := c.subTrie.Subscribe("c3", "metrics/cpu", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	subs, err := c.GetSubscribersForTopic(context.Background(), "sensors/node1/temp")
	if err != nil {
		t.Fatalf("GetSubscribersForTopic failed: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 subscriber, got %d", len(subs))
	}
	if subs[0].ClientID != "c1" {
		t.Fatalf("expected client c1, got %q", subs[0].ClientID)
	}

	subs, err = c.GetSubscribersForTopic(context.Background(), "alerts/critical/eu")
	if err != nil {
		t.Fatalf("GetSubscribersForTopic failed: %v", err)
	}
	if len(subs) != 1 || subs[0].ClientID != "c2" {
		t.Fatalf("expected alerts subscriber c2, got %#v", subs)
	}
}

func TestQueueConsumerCacheIndexingAndList(t *testing.T) {
	c := &EtcdCluster{
		queueConsumersAll:     make(map[string]*QueueConsumerInfo),
		queueConsumersByQueue: make(map[string]map[string]*QueueConsumerInfo),
		queueConsumersByGroup: make(map[string]map[string]map[string]*QueueConsumerInfo),
	}

	c.upsertQueueConsumerCache(&QueueConsumerInfo{
		QueueName:  "orders",
		GroupID:    "workers",
		ConsumerID: "c1",
	})
	c.upsertQueueConsumerCache(&QueueConsumerInfo{
		QueueName:  "orders",
		GroupID:    "workers/eu",
		ConsumerID: "c2",
	})
	c.upsertQueueConsumerCache(&QueueConsumerInfo{
		QueueName:  "payments",
		GroupID:    "workers",
		ConsumerID: "c3",
	})

	orders, err := c.ListQueueConsumers(context.Background(), "orders")
	if err != nil {
		t.Fatalf("ListQueueConsumers failed: %v", err)
	}
	if len(orders) != 2 {
		t.Fatalf("expected 2 order consumers, got %d", len(orders))
	}

	workersEU, err := c.ListQueueConsumersByGroup(context.Background(), "orders", "workers/eu")
	if err != nil {
		t.Fatalf("ListQueueConsumersByGroup failed: %v", err)
	}
	if len(workersEU) != 1 || workersEU[0].ConsumerID != "c2" {
		t.Fatalf("expected one workers/eu consumer c2, got %#v", workersEU)
	}

	all, err := c.ListAllQueueConsumers(context.Background())
	if err != nil {
		t.Fatalf("ListAllQueueConsumers failed: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 consumers, got %d", len(all))
	}

	c.removeQueueConsumerCache("orders", "workers/eu", "c2")
	workersEU, err = c.ListQueueConsumersByGroup(context.Background(), "orders", "workers/eu")
	if err != nil {
		t.Fatalf("ListQueueConsumersByGroup failed: %v", err)
	}
	if len(workersEU) != 0 {
		t.Fatalf("expected workers/eu group to be empty after delete, got %d", len(workersEU))
	}
}

func TestParseQueueConsumerKeyWithSlashesInGroup(t *testing.T) {
	key := queueConsumersPrefix + "orders/workers/region/eu/c1"
	queueName, groupID, consumerID, ok := parseQueueConsumerKey(key)
	if !ok {
		t.Fatalf("expected parse to succeed")
	}
	if queueName != "orders" {
		t.Fatalf("unexpected queueName: %q", queueName)
	}
	if groupID != "workers/region/eu" {
		t.Fatalf("unexpected groupID: %q", groupID)
	}
	if consumerID != "c1" {
		t.Fatalf("unexpected consumerID: %q", consumerID)
	}
}

func TestParseSessionOwnerKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		clientID string
		ok       bool
	}{
		{
			name:     "valid key",
			key:      sessionsPrefix + "client-1/owner",
			clientID: "client-1",
			ok:       true,
		},
		{
			name:     "client id with slash",
			key:      sessionsPrefix + "tenant/a/client-1/owner",
			clientID: "tenant/a/client-1",
			ok:       true,
		},
		{
			name: "missing owner suffix",
			key:  sessionsPrefix + "client-1",
			ok:   false,
		},
		{
			name: "missing client id",
			key:  sessionsPrefix + "/owner",
			ok:   false,
		},
		{
			name: "wrong prefix",
			key:  "/other/client-1/owner",
			ok:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientID, ok := parseSessionOwnerKey(tc.key)
			if ok != tc.ok {
				t.Fatalf("expected ok=%t, got %t", tc.ok, ok)
			}
			if clientID != tc.clientID {
				t.Fatalf("expected clientID=%q, got %q", tc.clientID, clientID)
			}
		})
	}
}
