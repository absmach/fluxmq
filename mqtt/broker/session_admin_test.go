// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
)

func TestPersistSessionInfoTracksConnectedState(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	s, _, err := b.CreateSession("persisted-client", 5, session.Options{
		CleanStart:     false,
		KeepAlive:      30 * time.Second,
		ReceiveMaximum: 10,
	})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	storedBefore, err := store.Sessions().Get("persisted-client")
	if err != nil {
		t.Fatalf("failed to load session before connect: %v", err)
	}
	if storedBefore.Connected {
		t.Fatalf("expected stored session to start disconnected")
	}

	if err := s.Connect(&mockConnection{}); err != nil {
		t.Fatalf("failed to connect session: %v", err)
	}

	b.persistSessionInfo(s)

	storedAfter, err := store.Sessions().Get("persisted-client")
	if err != nil {
		t.Fatalf("failed to load session after connect: %v", err)
	}
	if !storedAfter.Connected {
		t.Fatalf("expected connected session to be persisted as connected")
	}
	if storedAfter.ConnectedAt.IsZero() {
		t.Fatalf("expected connected_at to be persisted")
	}
}

func TestPersistSessionInfoNilGuards(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	// nil session should not panic
	b.persistSessionInfo(nil)

	// nil session store should not panic
	b.stores.sessions = nil
	s, _, err := b.CreateSession("x", 5, session.Options{CleanStart: true, KeepAlive: 10 * time.Second})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	b.persistSessionInfo(s)
}

func TestListSessionsFilterByState(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	tests := []struct {
		name      string
		state     string
		wantCount int
	}{
		{"all", "", 3},
		{"connected", "connected", 2},
		{"disconnected", "disconnected", 1},
		{"explicit all", "all", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessions, _, err := b.ListSessions(context.Background(), SessionListFilter{State: tt.state})
			if err != nil {
				t.Fatalf("ListSessions: %v", err)
			}
			if len(sessions) != tt.wantCount {
				t.Fatalf("expected %d sessions, got %d", tt.wantCount, len(sessions))
			}
		})
	}
}

func TestListSessionsFilterByPrefix(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	sessions, _, err := b.ListSessions(context.Background(), SessionListFilter{Prefix: "client-a"})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session with prefix client-a, got %d", len(sessions))
	}
	if sessions[0].ClientID != "client-a" {
		t.Fatalf("expected client-a, got %q", sessions[0].ClientID)
	}
}

func TestListSessionsPagination(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	// First page
	page1, token1, err := b.ListSessions(context.Background(), SessionListFilter{Limit: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 sessions on page 1, got %d", len(page1))
	}
	if token1 == "" {
		t.Fatal("expected non-empty page token after page 1")
	}

	// Second page
	page2, token2, err := b.ListSessions(context.Background(), SessionListFilter{Limit: 2, PageToken: token1})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected 1 session on page 2, got %d", len(page2))
	}
	if token2 != "" {
		t.Fatalf("expected empty page token after last page, got %q", token2)
	}

	// Verify no overlap
	seen := make(map[string]bool)
	for _, s := range page1 {
		seen[s.ClientID] = true
	}
	for _, s := range page2 {
		if seen[s.ClientID] {
			t.Fatalf("duplicate client %q across pages", s.ClientID)
		}
	}
}

func TestListSessionsSortedByClientID(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	sessions, _, err := b.ListSessions(context.Background(), SessionListFilter{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	for i := 1; i < len(sessions); i++ {
		if sessions[i].ClientID <= sessions[i-1].ClientID {
			t.Fatalf("sessions not sorted: %q <= %q", sessions[i].ClientID, sessions[i-1].ClientID)
		}
	}
}

func TestListSessionsStripsSubscriptionsFromListView(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	sessions, _, err := b.ListSessions(context.Background(), SessionListFilter{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	for _, s := range sessions {
		if s.Subscriptions != nil {
			t.Fatalf("expected nil Subscriptions in list view for %q", s.ClientID)
		}
	}
}

func TestListSessionsZeroLimitReturnsAllSessions(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	opts := session.Options{
		CleanStart:     false,
		KeepAlive:      30 * time.Second,
		ReceiveMaximum: 10,
	}

	const total = 60
	for i := 0; i < total; i++ {
		clientID := fmt.Sprintf("client-%03d", i)
		if _, _, err := b.CreateSession(clientID, 5, opts); err != nil {
			t.Fatalf("CreateSession(%q): %v", clientID, err)
		}
	}

	sessions, token, err := b.ListSessions(context.Background(), SessionListFilter{Limit: 0})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != total {
		t.Fatalf("expected %d sessions, got %d", total, len(sessions))
	}
	if token != "" {
		t.Fatalf("expected empty page token with no limit, got %q", token)
	}
}

func TestGetSessionSnapshotLive(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	snap, err := b.GetSessionSnapshot(context.Background(), "client-a")
	if err != nil {
		t.Fatalf("GetSessionSnapshot: %v", err)
	}
	if snap.ClientID != "client-a" {
		t.Fatalf("expected client-a, got %q", snap.ClientID)
	}
	if !snap.Connected {
		t.Fatal("expected connected")
	}
	if snap.SubscriptionCount != 1 {
		t.Fatalf("expected 1 subscription, got %d", snap.SubscriptionCount)
	}
	if len(snap.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription detail, got %d", len(snap.Subscriptions))
	}
}

func TestGetSessionSnapshotDisconnected(t *testing.T) {
	b, cleanup := newTestBrokerWithSessions(t)
	defer cleanup()

	snap, err := b.GetSessionSnapshot(context.Background(), "client-c")
	if err != nil {
		t.Fatalf("GetSessionSnapshot: %v", err)
	}
	if snap.Connected {
		t.Fatal("expected disconnected")
	}
	if snap.State != "disconnected" {
		t.Fatalf("expected state 'disconnected', got %q", snap.State)
	}
}

func TestGetSessionSnapshotNotFound(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	_, err := b.GetSessionSnapshot(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}

func TestGetSessionSnapshotEmptyClientID(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	_, err := b.GetSessionSnapshot(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty client ID")
	}
}

// newTestBrokerWithSessions creates a broker with 3 sessions:
//   - client-a: connected, subscribed to "a/topic"
//   - client-b: connected, subscribed to "b/topic"
//   - client-c: disconnected, subscribed to "c/topic"
func newTestBrokerWithSessions(t *testing.T) (*Broker, func()) {
	t.Helper()

	store := memory.New()
	b := NewBroker(store, nil)

	opts := session.Options{
		CleanStart:     false,
		KeepAlive:      30 * time.Second,
		ReceiveMaximum: 10,
	}

	for _, tc := range []struct {
		id        string
		connected bool
		filter    string
	}{
		{"client-a", true, "a/topic"},
		{"client-b", true, "b/topic"},
		{"client-c", false, "c/topic"},
	} {
		s, _, err := b.CreateSession(tc.id, 5, opts)
		if err != nil {
			t.Fatalf("CreateSession(%q): %v", tc.id, err)
		}
		if err := store.Subscriptions().Add(&storage.Subscription{
			ClientID: tc.id,
			Filter:   tc.filter,
			QoS:      1,
		}); err != nil {
			t.Fatalf("failed to store subscription for %q: %v", tc.id, err)
		}
		s.AddSubscription(tc.filter, storage.SubscribeOptions{})

		if err := s.Connect(&mockConnection{}); err != nil {
			t.Fatalf("Connect(%q): %v", tc.id, err)
		}
		b.persistSessionInfo(s)

		if !tc.connected {
			if err := s.Disconnect(true); err != nil {
				t.Fatalf("Disconnect(%q): %v", tc.id, err)
			}
			// Wait briefly for async disconnect callback to persist state
			time.Sleep(10 * time.Millisecond)
		}
	}

	return b, func() { b.Close() }
}
