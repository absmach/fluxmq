// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/stretchr/testify/require"
)

type cleanupSpyCluster struct {
	cluster.Cluster
	mu                          sync.Mutex
	owner                       bool
	acquires                    int
	releases                    int
	removeAllSubscriptionsCalls int
}

func (c *cleanupSpyCluster) NodeID() string { return "node-a" }

func (c *cleanupSpyCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.owner {
		return "", false, nil
	}
	return c.NodeID(), true, nil
}

func (c *cleanupSpyCluster) AcquireSession(context.Context, string, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owner = true
	c.acquires++
	return nil
}

func (c *cleanupSpyCluster) ReleaseSession(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owner = false
	c.releases++
	return nil
}

func (c *cleanupSpyCluster) RemoveAllSubscriptions(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeAllSubscriptionsCalls++
	return nil
}

func (c *cleanupSpyCluster) snapshot() (owner bool, acquires, releases, removeAllSubscriptionsCalls int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner, c.acquires, c.releases, c.removeAllSubscriptionsCalls
}

func cleanV3Connect(clientID string) *v3.Connect {
	connect := v3Connect(clientID)
	connect.CleanSession = true
	return connect
}

func TestHandleConnect_CleanSessionReconnectKeepsReplacement(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	h := newV3Handler(b)

	const clientID = "clean-reconnect"
	oldConn := newBlockingConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		_ = h.HandleConnect(context.Background(), oldConn, cleanV3Connect(clientID))
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old clean session connected")
	oldSession := b.sessionsMap.Get(clientID)
	require.NotNil(t, oldSession)
	oldDisconnectHandled := make(chan struct{})
	oldSession.SetOnDisconnect(func(s *session.Session, graceful bool) {
		b.handleDisconnect(s, graceful)
		close(oldDisconnectHandled)
	})

	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		_ = h.HandleConnect(context.Background(), newConn, cleanV3Connect(clientID))
	}()

	oldWG.Wait()
	select {
	case <-oldDisconnectHandled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for old clean session disconnect callback")
	}
	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "replacement clean session remains current")
	require.False(t, newConn.closed.Load())

	newConn.Close()
	newWG.Wait()
}

func TestHandleDisconnect_StaleCleanSessionDoesNotDeleteReplacementClusterState(t *testing.T) {
	cl := &cleanupSpyCluster{}
	b := NewBroker(nil, cl)
	defer b.Close()

	const clientID = "cluster-clean-reconnect"
	oldSession, _, err := b.CreateSession(clientID, 4, session.Options{CleanStart: true})
	require.NoError(t, err)

	replacement, _, err := b.CreateSession(clientID, 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.NotSame(t, oldSession, replacement)
	require.Same(t, replacement, b.sessionsMap.Get(clientID))

	owner, acquires, releases, removeAllSubscriptionsCalls := cl.snapshot()
	require.True(t, owner)
	require.Equal(t, 2, acquires)
	require.Equal(t, 1, releases)
	require.Equal(t, 1, removeAllSubscriptionsCalls)

	// Model the old Session.Disconnect callback starting after the replacement
	// was installed. It must not touch client-ID-scoped cluster state.
	b.handleDisconnect(oldSession, false)

	require.Same(t, replacement, b.sessionsMap.Get(clientID))
	owner, acquires, releases, removeAllSubscriptionsCalls = cl.snapshot()
	require.True(t, owner)
	require.Equal(t, 2, acquires)
	require.Equal(t, 1, releases)
	require.Equal(t, 1, removeAllSubscriptionsCalls)
}
