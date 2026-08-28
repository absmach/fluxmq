// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	identityA = "entity-a"
	identityB = "entity-b"
)

// takeoverCluster reports the session as owned by another node and hands back
// a fixed captured state, the way a real takeover does once the previous owner
// has closed its copy.
type takeoverCluster struct {
	cluster.Cluster
	state     *clusterv1.SessionState
	remoteID  string
	takeovers int
	released  int
}

func (c *takeoverCluster) NodeID() string { return "node-local" }

func (c *takeoverCluster) GetSessionOwner(_ context.Context, clientID string) (string, bool, error) {
	if c.remoteID != "" && clientID != c.remoteID {
		return "", false, nil
	}
	return "node-remote", true, nil
}

// The guard is deliberately ignored: these cases cover the local defence that
// stands behind the owner-side check, for a takeover that hands back state the
// connecting principal may not use.
func (c *takeoverCluster) TakeoverSession(context.Context, string, string, string, *cluster.SessionIdentityGuard) (*clusterv1.SessionState, error) {
	c.takeovers++
	return c.state, nil
}

func (c *takeoverCluster) AcquireSession(context.Context, string, string) error { return nil }

func (c *takeoverCluster) ReleaseSession(context.Context, string) error {
	c.released++
	return nil
}

func (c *takeoverCluster) AddSubscription(context.Context, string, string, byte, storage.SubscribeOptions) error {
	return nil
}

func (c *takeoverCluster) RemoveAllSubscriptions(context.Context, string) error { return nil }

func (c *takeoverCluster) RoutePublish(context.Context, *message.Envelope) error { return nil }

func (c *takeoverCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	return nil, nil
}

// guardHonoringCluster answers a takeover the way a real owner does: it
// applies the guard to the state it holds and refuses to hand over a session
// bound to a different principal. Whether an unbound session may be adopted is
// left to the node that asked, which is the point of the split.
type guardHonoringCluster struct {
	cluster.Cluster
	state    *clusterv1.SessionState
	guard    *cluster.SessionIdentityGuard
	guarded  bool
	refusals int
}

func (c *guardHonoringCluster) NodeID() string { return "node-local" }

func (c *guardHonoringCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return "node-remote", true, nil
}

func (c *guardHonoringCluster) TakeoverSession(_ context.Context, _, _, _ string, guard *cluster.SessionIdentityGuard) (*clusterv1.SessionState, error) {
	c.guarded = guard != nil
	c.guard = guard
	if guard != nil && !session.IdentityAllows(c.state.GetExternalId(), guard.ExternalID, false) {
		c.refusals++
		return nil, cluster.ErrSessionIdentityMismatch
	}

	return c.state, nil
}

func (c *guardHonoringCluster) AcquireSession(context.Context, string, string) error { return nil }

func (c *guardHonoringCluster) ReleaseSession(context.Context, string) error { return nil }

func (c *guardHonoringCluster) AddSubscription(context.Context, string, string, byte, storage.SubscribeOptions) error {
	return nil
}

func (c *guardHonoringCluster) RemoveAllSubscriptions(context.Context, string) error { return nil }

func (c *guardHonoringCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	return nil, nil
}

func migratedState(externalID string) *clusterv1.SessionState {
	return &clusterv1.SessionState{
		ExternalId:     externalID,
		ExpiryInterval: 300,
		Subscriptions:  []*clusterv1.Subscription{{Filter: "migrated/topic", Qos: 1}},
	}
}

func TestCreateSessionTakeoverIdentity(t *testing.T) {
	const clientID = "migrating-client"

	t.Run("matching identity inherits the migrated session", func(t *testing.T) {
		cl := &takeoverCluster{state: migratedState(identityA)}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityA}, false)
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Equal(t, identityA, s.ExternalIdentity())
		assert.Contains(t, s.GetSubscriptions(), "migrated/topic")
		assert.Zero(t, cl.released, "an inherited session keeps its ownership here")
	})

	t.Run("another identity is rejected and the session is kept for its owner", func(t *testing.T) {
		cl := &takeoverCluster{state: migratedState(identityA)}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, created, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityB}, false)
		require.ErrorIs(t, err, cluster.ErrSessionIdentityMismatch)
		require.Nil(t, s)
		require.False(t, created)

		// The previous owner has already closed its copy, so dropping the
		// state here would lose it. It stays, bound to its own principal.
		preserved := b.Get(clientID)
		require.NotNil(t, preserved, "the migrated session must remain reachable")
		assert.Equal(t, identityA, preserved.ExternalIdentity())
		assert.False(t, preserved.GetDisconnectedAt().IsZero(), "expiry must run from the migration")
		assert.Zero(t, cl.released, "ownership follows the state that is still here")
	})

	t.Run("a migrated session with no expiry is not kept", func(t *testing.T) {
		state := migratedState(identityA)
		state.ExpiryInterval = 0
		cl := &takeoverCluster{state: state}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityB, ExpiryInterval: 300}, false)
		require.NoError(t, err, "there is nothing left to protect once the owner's connection is gone")
		require.NotNil(t, s)
		assert.Equal(t, identityB, s.ExternalIdentity())
		assert.Empty(t, s.GetSubscriptions())
	})

	t.Run("clean start does not inherit the migrated session", func(t *testing.T) {
		cl := &takeoverCluster{state: migratedState(identityA)}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityB, CleanStart: true}, false)
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Equal(t, identityB, s.ExternalIdentity(), "Clean Start starts from the connecting principal")
		assert.Empty(t, s.GetSubscriptions(), "Clean Start must not inherit subscriptions")
	})

	t.Run("bound connect discards an unbound migrated session", func(t *testing.T) {
		cl := &takeoverCluster{state: migratedState("")}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityA}, true)
		require.NoError(t, err, "a client must not be locked out of its own client ID")
		require.NotNil(t, s)
		assert.Equal(t, identityA, s.ExternalIdentity())
		assert.Empty(t, s.GetSubscriptions(), "state bound to no principal is not inherited")
	})
}

func TestCreateSessionRemoteCleanStartRetiresTransferredWill(t *testing.T) {
	const clientID = "remote-clean-start"
	const oldWillTopic = "clients/remote-clean-start/old"
	const newWillTopic = "clients/remote-clean-start/new"

	cl := &takeoverCluster{
		remoteID: clientID,
		state: &clusterv1.SessionState{
			ExpiryInterval: 300,
			Will: &clusterv1.WillMessage{
				Topic:   oldWillTopic,
				Payload: []byte("old-offline"),
				Delay:   60,
			},
		},
	}
	b := NewBroker(memory.New(), cl)
	t.Cleanup(func() { _ = b.Close() })

	sub, _, err := b.CreateSession("remote-clean-start-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, oldWillTopic, 0, storage.SubscribeOptions{}))

	incomingWill := &storage.WillMessage{
		ClientID: clientID,
		Topic:    newWillTopic,
		Payload:  []byte("new-offline"),
	}
	s, created, err := b.CreateSession(clientID, 5, session.Options{
		CleanStart: true,
		Will:       incomingWill,
	})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, newWillTopic, s.GetWill().Topic, "the transferred Will must not replace the new CONNECT's Will")

	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == oldWillTopic {
				return string(pub.Payload) == "old-offline"
			}
		}
		return false
	}, "Clean Start publishes the transferred delayed Will immediately")
}

func TestCreateSessionPersistedIdentity(t *testing.T) {
	const clientID = "returning-client"

	newBrokerWithStoredSession := func(t *testing.T, externalID string) *Broker {
		t.Helper()

		store := memory.New()
		require.NoError(t, store.Sessions().Save(&storage.Session{
			ClientID:       clientID,
			ExternalID:     externalID,
			Version:        5,
			ExpiryInterval: 300,
		}))
		require.NoError(t, store.Subscriptions().Add(&storage.Subscription{
			ClientID: clientID,
			Filter:   "stored/topic",
			QoS:      1,
		}))

		b := NewBroker(store, nil)
		t.Cleanup(func() { _ = b.Close() })

		return b
	}

	t.Run("same identity resumes the persisted session", func(t *testing.T) {
		b := newBrokerWithStoredSession(t, identityA)

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityA}, false)
		require.NoError(t, err)
		assert.Equal(t, identityA, s.ExternalIdentity())
		assert.Contains(t, s.GetSubscriptions(), "stored/topic")
	})

	t.Run("another identity is rejected before any state is loaded", func(t *testing.T) {
		b := newBrokerWithStoredSession(t, identityA)

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityB}, false)
		require.ErrorIs(t, err, cluster.ErrSessionIdentityMismatch)
		require.Nil(t, s)
		assert.Nil(t, b.Get(clientID), "a rejected CONNECT must not leave a session behind")
	})

	t.Run("an unbound session is adopted by an unbound listener", func(t *testing.T) {
		b := newBrokerWithStoredSession(t, "")

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityA}, false)
		require.NoError(t, err)
		assert.Equal(t, identityA, s.ExternalIdentity())
		assert.Contains(t, s.GetSubscriptions(), "stored/topic")
	})

	t.Run("a bound connect discards an unbound session", func(t *testing.T) {
		b := newBrokerWithStoredSession(t, "")

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityA}, true)
		require.NoError(t, err, "a client must not be locked out of its own client ID")
		assert.Equal(t, identityA, s.ExternalIdentity())
		assert.Empty(t, s.GetSubscriptions(), "subscriptions made before binding are not inherited")

		subs, err := b.stores.subscriptions.GetForClient(clientID)
		require.NoError(t, err)
		assert.Empty(t, subs, "the discarded state must not survive to the next reconnect")
	})
}

func TestCreateSessionConcurrentIdentityBinding(t *testing.T) {
	const clientID = "contended-client"

	store := memory.New()
	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))

	b := NewBroker(store, nil)
	t.Cleanup(func() { _ = b.Close() })

	// Both principals may adopt an unbound session, but only one may end up
	// bound to it: a session that authorizes one connection under the other
	// principal's identity is the failure this guards.
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted []string
	)
	for _, externalID := range []string{identityA, identityB} {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if _, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: externalID}, false); err == nil {
				mu.Lock()
				accepted = append(accepted, externalID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Len(t, accepted, 1, "only one principal may bind an unbound session")
	s := b.Get(clientID)
	require.NotNil(t, s)
	assert.Equal(t, accepted[0], s.ExternalIdentity())
}

func TestCreateSessionTakeoverAgainstGuardingOwner(t *testing.T) {
	const clientID = "migrating-client"

	t.Run("clean start for another principal is not guarded", func(t *testing.T) {
		cl := &guardHonoringCluster{state: migratedState(identityA)}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityB, CleanStart: true}, false)
		require.NoError(t, err, "Clean Start is the documented way to take a client ID over deliberately")
		require.NotNil(t, s)
		assert.False(t, cl.guarded, "Clean Start inherits nothing, so the owner has nothing to protect")
		assert.Zero(t, cl.refusals)
		assert.Equal(t, identityB, s.ExternalIdentity())
		assert.Empty(t, s.GetSubscriptions())
	})

	t.Run("a bound connect adopts a client ID held by an unbound remote session", func(t *testing.T) {
		cl := &guardHonoringCluster{state: migratedState("")}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityA}, true)
		require.NoError(t, err, "the owner must not apply the arriving node's binding policy")
		require.NotNil(t, s)
		require.NotNil(t, cl.guard)
		assert.True(t, cl.guard.RequireBound)
		assert.Zero(t, cl.refusals)
		assert.Equal(t, identityA, s.ExternalIdentity())
		assert.Empty(t, s.GetSubscriptions(), "state bound to no principal is not inherited")
	})

	t.Run("a session bound to another principal is refused by its owner", func(t *testing.T) {
		cl := &guardHonoringCluster{state: migratedState(identityA)}
		b := NewBroker(memory.New(), cl)
		t.Cleanup(func() { _ = b.Close() })

		s, _, err := b.CreateSessionForIdentity(clientID, 5, session.Options{ExternalID: identityB}, false)
		require.ErrorIs(t, err, cluster.ErrSessionIdentityMismatch)
		require.Nil(t, s)
		assert.Equal(t, 1, cl.refusals)
		assert.Nil(t, b.Get(clientID), "the owner keeps its session; nothing lands here")
	})
}

func TestGetSessionStateAndCloseTransfersUnboundSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { _ = b.Close() })

	s, _, err := b.CreateSession("unbound-client", 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.Empty(t, s.ExternalIdentity())

	// The arriving node asked for a bound identity. That policy is applied
	// there, after the transfer, not here.
	state, err := b.GetSessionStateAndClose(context.Background(), "unbound-client", &cluster.SessionIdentityGuard{
		ExternalID:   identityA,
		RequireBound: true,
	})
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Nil(t, b.Get("unbound-client"))
}

func TestCreateSessionBoundConnectDiscardsUnboundLocalSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { _ = b.Close() })

	existing, _, err := b.CreateSession("legacy-client", 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	existing.AddSubscription("legacy/topic", storage.SubscribeOptions{})

	s, created, err := b.CreateSessionForIdentity("legacy-client", 5, session.Options{ExternalID: identityA}, true)
	require.NoError(t, err, "a live unbound session must not lock a bound client out of its client ID")
	require.True(t, created)
	assert.NotSame(t, existing, s, "the unbound session is replaced, not adopted")
	assert.Equal(t, identityA, s.ExternalIdentity())
	assert.Empty(t, s.GetSubscriptions())
}
