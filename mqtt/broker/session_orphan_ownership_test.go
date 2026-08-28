// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

var errSessionOwnedElsewhere = errors.New("session owned by another node")

// sharedClusterState is the one etcd both nodes in these tests talk to: a
// session owner key and the routing entries keyed by client ID.
type sharedClusterState struct {
	mu    sync.Mutex
	owner string
	subs  map[string][]*storage.Subscription
}

func newSharedClusterState() *sharedClusterState {
	return &sharedClusterState{subs: make(map[string][]*storage.Subscription)}
}

// raceCluster is one node's view of that state.
//
// GetSessionOwner always reports no owner. It stands in for the window between
// a node reading ownership and another node acquiring it: both nodes see the
// client ID as free, and only AcquireSession is authoritative about who got it.
type raceCluster struct {
	cluster.Cluster
	state  *sharedClusterState
	nodeID string
}

func (c *raceCluster) NodeID() string { return c.nodeID }

func (c *raceCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (c *raceCluster) AcquireSession(_ context.Context, _, nodeID string) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	if c.state.owner != "" && c.state.owner != nodeID {
		return errSessionOwnedElsewhere
	}
	c.state.owner = nodeID

	return nil
}

func (c *raceCluster) ReleaseSession(context.Context, string) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	if c.state.owner == c.nodeID {
		c.state.owner = ""
	}

	return nil
}

func (c *raceCluster) GetSubscriptionsForClient(_ context.Context, clientID string) ([]*storage.Subscription, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	return slices.Clone(c.state.subs[clientID]), nil
}

func (c *raceCluster) AddSubscription(_ context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	c.state.subs[clientID] = append(c.state.subs[clientID], &storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	})

	return nil
}

func (c *raceCluster) RemoveAllSubscriptions(_ context.Context, clientID string) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	delete(c.state.subs, clientID)

	return nil
}

func (c *raceCluster) subscriptions(clientID string) []*storage.Subscription {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	return slices.Clone(c.state.subs[clientID])
}

// A CONNECT that loses the ownership race must leave the winner's routes alone.
//
// Both nodes read the client ID as unowned, so the loser reaches createSession
// believing the session is its own to clean up. Cleaning up before acquiring
// ownership deletes every cluster route under that client ID — including the
// ones the winner has just subscribed — and the loser's own CONNECT then fails,
// leaving nobody to put them back. Cross-node publishes miss a live subscriber.
func TestOrphanCleanupDoesNotRemoveWinningNodesSubscriptions(t *testing.T) {
	const clientID = "contended-client"
	const filter = "sensors/+/temp"

	state := newSharedClusterState()
	// A route left behind by an earlier session under this client ID: without
	// one there is nothing for the cleanup to find and nothing to delete.
	state.subs[clientID] = []*storage.Subscription{{ClientID: clientID, Filter: "legacy/#", QoS: 0}}

	winnerCluster := &raceCluster{state: state, nodeID: "node-winner"}
	winner := NewBroker(memory.New(), winnerCluster)
	defer winner.Close()

	loserCluster := &raceCluster{state: state, nodeID: "node-loser"}
	loser := NewBroker(memory.New(), loserCluster)
	defer loser.Close()

	// The winner takes the client ID and subscribes.
	s, created, err := winner.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, winner.subscribe(s, filter, 1, storage.SubscribeOptions{}))
	require.Len(t, winnerCluster.subscriptions(clientID), 1, "the winner's route is in the cluster")

	// The loser proceeds on its stale ownership read.
	got, created, err := loser.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.ErrorIs(t, err, errSessionOwnedElsewhere)
	require.Nil(t, got)
	require.False(t, created)

	subs := loserCluster.subscriptions(clientID)
	require.Len(t, subs, 1, "the loser must not remove the winner's cluster subscriptions")
	require.Equal(t, filter, subs[0].Filter)
	require.Contains(t, winner.sessionsMap.Get(clientID).GetSubscriptions(), filter, "the winner keeps its subscription")
}

// The winner's own cleanup is legitimate: it holds the client ID, so the routes
// a previous session left there are its to clear.
func TestOwningNodeClearsOrphanedClusterSubscriptions(t *testing.T) {
	const clientID = "sole-owner"

	state := newSharedClusterState()
	state.subs[clientID] = []*storage.Subscription{{ClientID: clientID, Filter: "legacy/#", QoS: 0}}

	cl := &raceCluster{state: state, nodeID: "node-winner"}
	b := NewBroker(memory.New(), cl)
	defer b.Close()

	s, created, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, s.GetSubscriptions(), "a fresh session must not inherit the orphaned route")
	require.Empty(t, cl.subscriptions(clientID), "the orphaned route is cleared by the node that owns the client ID")
}
