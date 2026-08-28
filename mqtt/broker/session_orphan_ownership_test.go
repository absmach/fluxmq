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

// orphanFilter is the route a previous session left under a contended client ID.
const orphanFilter = "legacy/#"

// sharedClusterState is the one etcd both nodes in these tests talk to: a
// session owner key and the routing entries keyed by client ID.
type sharedClusterState struct {
	mu    sync.Mutex
	owner string
	subs  map[string][]*storage.Subscription
	// calls records the cluster operations in the order they were made, so a
	// test can pin what a CONNECT announced and when.
	calls []string
}

func newSharedClusterState() *sharedClusterState {
	return &sharedClusterState{subs: make(map[string][]*storage.Subscription)}
}

// record must be called with the state lock held.
func (s *sharedClusterState) record(nodeID, op string) {
	s.calls = append(s.calls, nodeID+":"+op)
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
	c.state.record(c.nodeID, "acquire")

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
	c.state.record(c.nodeID, "remove_all_subscriptions")
	delete(c.state.subs, clientID)

	return nil
}

func (c *raceCluster) callLog() []string {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	return slices.Clone(c.state.calls)
}

func (c *raceCluster) currentOwner() string {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	return c.state.owner
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
	state.subs[clientID] = []*storage.Subscription{{ClientID: clientID, Filter: orphanFilter, QoS: 0}}

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
	state.subs[clientID] = []*storage.Subscription{{ClientID: clientID, Filter: orphanFilter, QoS: 0}}

	cl := &raceCluster{state: state, nodeID: "node-winner"}
	b := NewBroker(memory.New(), cl)
	defer b.Close()

	s, created, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, s.GetSubscriptions(), "a fresh session must not inherit the orphaned route")
	require.Empty(t, cl.subscriptions(clientID), "the orphaned route is cleared by the node that owns the client ID")
}

// Announcing ownership is visible to the rest of the cluster, so a CONNECT that
// is going to be rejected must never announce it. Taking the client ID before
// the persisted identity is checked lets a mismatched client hold it briefly: a
// legitimate node can begin a takeover against that announcement, block on this
// node's client-ID lock, and find no session here once the rejected CONNECT has
// released ownership. Its takeover then finalizes against an absent owner and
// completes with no state, while the durable session stays behind on this node.
func TestRejectedIdentityNeverAcquiresOwnership(t *testing.T) {
	const clientID = "mismatched-identity"

	store := memory.New()
	cl := &raceCluster{state: newSharedClusterState(), nodeID: "node-a"}
	b := NewBroker(store, cl)
	defer b.Close()

	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExternalID:     identityA,
		ExpiryInterval: 300,
	}))

	got, created, err := b.CreateSessionForIdentity(clientID, 5, session.Options{
		ExternalID:     identityB,
		ExpiryInterval: 300,
	}, false)
	require.ErrorIs(t, err, cluster.ErrSessionIdentityMismatch)
	require.Nil(t, got)
	require.False(t, created)

	require.NotContains(t, cl.callLog(), "node-a:acquire", "a rejected CONNECT must not announce ownership")
	require.Empty(t, cl.currentOwner())

	stored, err := store.Sessions().Get(clientID)
	require.NoError(t, err)
	require.Equal(t, identityA, stored.ExternalID, "the rejected CONNECT leaves the owner's session alone")
}

// The counterpart: state this CONNECT does discard is deleted, and only once
// the client ID is owned here, so the deletion never reaches a session that is
// live on another node.
func TestDiscardedSessionIsPurgedAfterOwnership(t *testing.T) {
	const clientID = "unbound-discarded"

	store := memory.New()
	cl := &raceCluster{state: newSharedClusterState(), nodeID: "node-a"}
	b := NewBroker(store, cl)
	defer b.Close()

	// A session predating certificate binding: no principal on the record.
	cl.state.subs[clientID] = []*storage.Subscription{{ClientID: clientID, Filter: orphanFilter, QoS: 0}}
	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))
	require.NoError(t, store.Subscriptions().Add(&storage.Subscription{
		ClientID: clientID,
		Filter:   orphanFilter,
		QoS:      1,
	}))

	s, created, err := b.CreateSessionForIdentity(clientID, 5, session.Options{
		ExternalID:     identityA,
		ExpiryInterval: 300,
	}, true)
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, s.GetSubscriptions(), "the discarded session's subscriptions are not inherited")

	subs, err := store.Subscriptions().GetForClient(clientID)
	require.NoError(t, err)
	require.Empty(t, subs, "the discarded session is purged")

	calls := cl.callLog()
	acquired := slices.Index(calls, "node-a:acquire")
	removed := slices.Index(calls, "node-a:remove_all_subscriptions")
	require.NotEqual(t, -1, acquired, "the accepted CONNECT takes the client ID")
	require.NotEqual(t, -1, removed, "the discarded session's cluster routes are cleared")
	require.Less(t, acquired, removed, "the client ID is owned before its cluster routes are deleted")
}
