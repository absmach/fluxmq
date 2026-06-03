// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "testing"

func TestConnectionSubscriptionsDeduplicatesConsumersByFilter(t *testing.T) {
	b := New(nil, nil)

	conn := &Connection{
		broker: b,
		connID: testConn1,
		channels: map[uint16]*Channel{
			1: {
				consumers: map[string]*consumer{
					"tag-1": {
						tag:   "tag-1",
						queue: testOrdersStar,
					},
					"tag-2": {
						tag:   "tag-2",
						queue: testOrdersStar,
					},
					"tag-3": {
						tag:       "tag-3",
						queue:     "$queue/jobs/#",
						queueName: "jobs",
						pattern:   "#",
					},
				},
			},
		},
	}
	b.registerConnection(testConn1, conn)

	subs := b.ConnectionSubscriptions(testConn1)
	if len(subs) != 2 {
		t.Fatalf("expected 2 unique subscriptions, got %d", len(subs))
	}

	if subs[0].ClientID != PrefixedClientID(testConn1) {
		t.Fatalf("expected prefixed client ID %q, got %q", PrefixedClientID(testConn1), subs[0].ClientID)
	}
	if subs[0].Filter != "$queue/jobs/#" {
		t.Fatalf("expected first filter $queue/jobs/#, got %q", subs[0].Filter)
	}
	if subs[1].Filter != testOrdersStar {
		t.Fatalf("expected second filter orders.*, got %q", subs[1].Filter)
	}
}

func TestListSubscriptionSnapshotsSortsAcrossConnections(t *testing.T) {
	b := New(nil, nil)

	b.registerConnection("conn-b", &Connection{
		broker: b,
		connID: "conn-b",
		channels: map[uint16]*Channel{
			1: {
				consumers: map[string]*consumer{
					"tag-b": {
						tag:   "tag-b",
						queue: "zulu.*",
					},
				},
			},
		},
	})
	b.registerConnection("conn-a", &Connection{
		broker: b,
		connID: "conn-a",
		channels: map[uint16]*Channel{
			1: {
				consumers: map[string]*consumer{
					"tag-a": {
						tag:   "tag-a",
						queue: "alpha.*",
					},
				},
			},
		},
	})

	subs := b.ListSubscriptionSnapshots()
	if len(subs) != 2 {
		t.Fatalf("expected 2 subscriptions, got %d", len(subs))
	}
	if subs[0].Filter != "alpha.*" {
		t.Fatalf("expected first filter alpha.*, got %q", subs[0].Filter)
	}
	if subs[1].Filter != "zulu.*" {
		t.Fatalf("expected second filter zulu.*, got %q", subs[1].Filter)
	}
}
