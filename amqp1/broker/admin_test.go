// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/amqp1/performatives"
	"github.com/stretchr/testify/require"
)

func TestConnectionSnapshotsTrackReceiverLifecycle(t *testing.T) {
	b := New(nil, nil, nil)
	c := &Connection{broker: b, containerID: "client", ctx: context.Background(), sessions: map[uint16]*Session{}}
	s := newSession(c, 0, 0, 16)
	c.sessions[0] = s
	b.registerConnection(c.containerID, c)
	defer b.unregisterConnection(c.containerID)
	require.Len(t, b.ConnectionSnapshots(), 1)
	snap, ok := b.ConnectionSnapshot("client")
	require.True(t, ok)
	require.Equal(t, PrefixedClientID("client"), snap.ClientID)
	require.False(t, snap.ConnectedAt.IsZero())
	require.Empty(t, snap.Subscriptions)
	l := newLink(s, &performatives.Attach{Name: "receiver", Role: true, Source: &performatives.Source{Address: "orders.*"}})
	s.links[0] = l
	// An attached link is only a subscription after registration succeeds.
	snap, _ = b.ConnectionSnapshot("client")
	require.Empty(t, snap.Subscriptions)
	l.subscribe()
	snap, _ = b.ConnectionSnapshot("client")
	require.Equal(t, []SubscriptionSnapshot{{Filter: "orders/+"}}, snap.Subscriptions)
	// Returned snapshots are detached copies.
	snap.Subscriptions[0].Filter = "changed"
	snap, _ = b.ConnectionSnapshot("client")
	require.Equal(t, "orders/+", snap.Subscriptions[0].Filter)
	l.detach()
	snap, _ = b.ConnectionSnapshot("client")
	require.Empty(t, snap.Subscriptions)
	c.closed.Store(true)
	require.Empty(t, b.ConnectionSnapshots())
	_, ok = b.ConnectionSnapshot("client")
	require.False(t, ok)
}

func TestConnectionSnapshotConcurrentLinkChanges(t *testing.T) {
	b := New(nil, nil, nil)
	c := &Connection{sessions: map[uint16]*Session{}}
	s := newSession(c, 0, 0, 16)
	c.sessions[0] = s
	b.registerConnection("race", c)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			l := &Link{}
			l.adminSubscription.Store(&SubscriptionSnapshot{Filter: "events/+"})
			s.linksMu.Lock()
			s.links[0] = l
			s.linksMu.Unlock()
			l.adminSubscription.Store(nil)
			s.linksMu.Lock()
			delete(s.links, 0)
			s.linksMu.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		b.ConnectionSnapshots()
	}
	wg.Wait()
}
