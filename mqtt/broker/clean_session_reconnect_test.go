// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/stretchr/testify/require"
)

type cleanupSpyCluster struct {
	cluster.Cluster
	mu         sync.Mutex
	owner      bool
	acquires   int
	releases   int
	removeAlls int
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
	c.removeAlls++
	return nil
}

func (c *cleanupSpyCluster) snapshot() (owner bool, acquires, releases, removeAlls int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner, c.acquires, c.releases, c.removeAlls
}

func cleanV3Connect(clientID string) *v3.Connect {
	return &v3.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    protocolNameMQTT,
		ProtocolVersion: 4,
		ClientID:        clientID,
		CleanSession:    true,
		KeepAlive:       60,
	}
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

	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		_ = h.HandleConnect(context.Background(), newConn, cleanV3Connect(clientID))
	}()

	oldWG.Wait()
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

	owner, acquires, releases, removeAlls := cl.snapshot()
	require.True(t, owner)
	require.Equal(t, 2, acquires)
	require.Equal(t, 1, releases)
	require.Equal(t, 1, removeAlls)

	// Model the old Session.Disconnect callback starting after the replacement
	// was installed. It must not touch client-ID-scoped cluster state.
	b.handleDisconnect(oldSession, false)

	require.Same(t, replacement, b.sessionsMap.Get(clientID))
	owner, acquires, releases, removeAlls = cl.snapshot()
	require.True(t, owner)
	require.Equal(t, 2, acquires)
	require.Equal(t, 1, releases)
	require.Equal(t, 1, removeAlls)
}
