// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
)

func TestListSubscriptionsFilterByState(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	tests := []struct {
		name      string
		state     string
		wantCount int
	}{
		{"defaults to all filters", "", 2},
		{"connected only", "connected", 1},
		{"disconnected only", "disconnected", 2},
		{"explicit all", "all", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subs, _, err := b.ListSubscriptions(SubscriptionListFilter{State: tt.state})
			if err != nil {
				t.Fatalf("ListSubscriptions: %v", err)
			}
			if len(subs) != tt.wantCount {
				t.Fatalf("expected %d subscriptions, got %d", tt.wantCount, len(subs))
			}
		})
	}
}

func TestListSubscriptionsAggregatesSubscriberCount(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	subs, _, err := b.ListSubscriptions(SubscriptionListFilter{State: "all"})
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}

	byFilter := make(map[string]SubscriptionSnapshot)
	for _, s := range subs {
		byFilter[s.Filter] = s
	}

	shared := byFilter["shared/topic"]
	if shared.SubscriberCount != 2 {
		t.Fatalf("expected 2 subscribers for shared/topic, got %d", shared.SubscriberCount)
	}
	if shared.MaxQoS != 2 {
		t.Fatalf("expected max QoS 2 for shared/topic, got %d", shared.MaxQoS)
	}
}

func TestListSubscriptionsPrefix(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	subs, _, err := b.ListSubscriptions(SubscriptionListFilter{
		State:  "all",
		Prefix: "shared/",
	})
	if err != nil {
		t.Fatalf("ListSubscriptions: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 subscription with prefix shared/, got %d", len(subs))
	}
	if subs[0].Filter != "shared/topic" {
		t.Fatalf("expected shared/topic, got %q", subs[0].Filter)
	}
}

func TestListSubscriptionsPagination(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	page1, token1, err := b.ListSubscriptions(SubscriptionListFilter{
		State: "all",
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1) != 1 {
		t.Fatalf("expected 1 subscription on page 1, got %d", len(page1))
	}
	if token1 == "" {
		t.Fatal("expected non-empty page token")
	}

	page2, token2, err := b.ListSubscriptions(SubscriptionListFilter{
		State:     "all",
		Limit:     1,
		PageToken: token1,
	})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected 1 subscription on page 2, got %d", len(page2))
	}
	if token2 != "" {
		t.Fatalf("expected empty page token on last page, got %q", token2)
	}

	if page1[0].Filter == page2[0].Filter {
		t.Fatal("pages returned duplicate filter")
	}
}

func TestListSubscriptionClientsByState(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	tests := []struct {
		name      string
		state     string
		wantCount int
	}{
		{"defaults to all", "", 2},
		{"connected", "connected", 1},
		{"disconnected", "disconnected", 1},
		{"all", "all", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clients, _, err := b.ListSubscriptionClients("shared/topic", tt.state, "", 0, "")
			if err != nil {
				t.Fatalf("ListSubscriptionClients: %v", err)
			}
			if len(clients) != tt.wantCount {
				t.Fatalf("expected %d clients, got %d", tt.wantCount, len(clients))
			}
		})
	}
}

func TestListSubscriptionClientsPrefix(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	clients, _, err := b.ListSubscriptionClients("shared/topic", "all", "sub-a", 0, "")
	if err != nil {
		t.Fatalf("ListSubscriptionClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("expected 1 client with prefix sub-a, got %d", len(clients))
	}
	if clients[0].ClientID != "sub-a" {
		t.Fatalf("expected sub-a, got %q", clients[0].ClientID)
	}
}

func TestListSubscriptionClientsPagination(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	page1, token1, err := b.ListSubscriptionClients("shared/topic", "all", "", 1, "")
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1) != 1 {
		t.Fatalf("expected 1 client on page 1, got %d", len(page1))
	}
	if token1 == "" {
		t.Fatal("expected non-empty page token")
	}

	page2, token2, err := b.ListSubscriptionClients("shared/topic", "all", "", 1, token1)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected 1 client on page 2, got %d", len(page2))
	}
	if token2 != "" {
		t.Fatalf("expected empty page token on last page, got %q", token2)
	}
	if page1[0].ClientID == page2[0].ClientID {
		t.Fatal("pages returned duplicate client")
	}
}

func TestListSubscriptionClientsNonexistentFilter(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	clients, _, err := b.ListSubscriptionClients("nonexistent/filter", "", "", 0, "")
	if err != nil {
		t.Fatalf("ListSubscriptionClients: %v", err)
	}
	if len(clients) != 0 {
		t.Fatalf("expected 0 clients for nonexistent filter, got %d", len(clients))
	}
}

func TestListSubscriptionClientsNilStore(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	b.stores.subscriptions = nil

	clients, token, err := b.ListSubscriptionClients("any/filter", "", "", 0, "")
	if err != nil {
		t.Fatalf("ListSubscriptionClients: %v", err)
	}
	if len(clients) != 0 {
		t.Fatalf("expected 0 clients with nil store, got %d", len(clients))
	}
	if token != "" {
		t.Fatalf("expected empty token with nil store, got %q", token)
	}
}

func TestListSubscriptionClientsQoS(t *testing.T) {
	b, cleanup := newTestBrokerWithSharedSubs(t)
	defer cleanup()

	clients, _, err := b.ListSubscriptionClients("shared/topic", "all", "", 0, "")
	if err != nil {
		t.Fatalf("ListSubscriptionClients: %v", err)
	}

	for _, c := range clients {
		if c.ClientID == "sub-a" && c.QoS != 1 {
			t.Fatalf("expected QoS 1 for sub-a, got %d", c.QoS)
		}
		if c.ClientID == "sub-b" && c.QoS != 2 {
			t.Fatalf("expected QoS 2 for sub-b, got %d", c.QoS)
		}
	}
}

// newTestBrokerWithSharedSubs creates a broker with subscription test data:
//   - sub-a: connected, subscribed to "shared/topic" (QoS 1)
//   - sub-b: disconnected, subscribed to "shared/topic" (QoS 2) and "only/b" (QoS 0)
func newTestBrokerWithSharedSubs(t *testing.T) (*Broker, func()) {
	t.Helper()

	store := memory.New()
	b := NewBroker(store, nil)

	opts := session.Options{
		CleanStart:     false,
		KeepAlive:      30 * time.Second,
		ReceiveMaximum: 10,
	}

	type subEntry struct {
		filter string
		qos    byte
	}

	for _, tc := range []struct {
		id        string
		connected bool
		subs      []subEntry
	}{
		{"sub-a", true, []subEntry{{"shared/topic", 1}}},
		{"sub-b", false, []subEntry{{"shared/topic", 2}, {"only/b", 0}}},
	} {
		s, _, err := b.CreateSession(tc.id, 5, opts)
		if err != nil {
			t.Fatalf("CreateSession(%q): %v", tc.id, err)
		}

		for _, sub := range tc.subs {
			if err := store.Subscriptions().Add(&storage.Subscription{
				ClientID: tc.id,
				Filter:   sub.filter,
				QoS:      sub.qos,
			}); err != nil {
				t.Fatalf("failed to store subscription for %q: %v", tc.id, err)
			}
			s.AddSubscription(sub.filter, storage.SubscribeOptions{})
		}

		if err := s.Connect(&mockConnection{}); err != nil {
			t.Fatalf("Connect(%q): %v", tc.id, err)
		}
		b.persistSessionInfo(s)

		if !tc.connected {
			if err := s.Disconnect(true); err != nil {
				t.Fatalf("Disconnect(%q): %v", tc.id, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	return b, func() { b.Close() }
}
