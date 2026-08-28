// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/require"
)

var errWillStoreUnavailable = errors.New("will store unavailable")

// faultyWillStore fails Get on demand, modelling a storage backend that becomes
// briefly unreachable while a CONNECT is in flight.
type faultyWillStore struct {
	storage.WillStore
	failGet atomic.Bool
}

func (w *faultyWillStore) Get(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	if w.failGet.Load() {
		return nil, errWillStoreUnavailable
	}

	return w.WillStore.Get(ctx, clientID)
}

// faultyStore serves a faultyWillStore in place of the backend's own.
type faultyStore struct {
	storage.Store
	wills *faultyWillStore
}

func (s *faultyStore) Wills() storage.WillStore { return s.wills }

func newFaultyStore() *faultyStore {
	inner := memory.New()

	return &faultyStore{Store: inner, wills: &faultyWillStore{WillStore: inner.Wills()}}
}

// A CONNECT that fails before attaching must not delete durable state.
// createSession reports a session rebuilt from storage as newly created, so a
// rollback that destroyed the session would let one failed CONNECT take a
// client's subscriptions and queued messages with it.
func TestAttachFailureKeepsRestoredDurableSession(t *testing.T) {
	store := newFaultyStore()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "restored-durable"
	const filter = "sensors/+/temp"

	// Seed the persisted session the reconnect will restore.
	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))
	require.NoError(t, store.Subscriptions().Add(&storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      1,
	}))

	store.wills.failGet.Store(true)

	s, created, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.True(t, created, "a restored session is still reported as created")

	_, err = b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.ErrorIs(t, err, errWillStoreUnavailable)

	b.releaseUnattachedSession(context.Background(), s, claim.epoch)

	require.Nil(t, b.sessionsMap.Get(clientID), "the unattached session is handed back")

	stored, err := store.Sessions().Get(clientID)
	require.NoError(t, err)
	require.NotNil(t, stored, "the persisted session must survive a failed CONNECT")

	subs, err := store.Subscriptions().GetForClient(clientID)
	require.NoError(t, err)
	require.Len(t, subs, 1, "persisted subscriptions must survive a failed CONNECT")
	require.Equal(t, filter, subs[0].Filter)
}

// Restoring a session moves its queued and inflight messages out of storage and
// into the session object, so a rollback that only hands back the object drops
// them: nothing is left in storage and the release frees the envelopes.
func TestAttachFailureKeepsRestoredMessages(t *testing.T) {
	store := newFaultyStore()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "restored-messages"
	const packetID = 7

	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))

	queued := message.New("sensors/1/temp", []byte("queued-payload"))
	require.NoError(t, store.Messages().Store(clientID+queuePrefix+"0", queued))
	message.Release(queued)

	pending := message.New("sensors/1/humidity", []byte("inflight-payload"))
	pending.BrokerMeta.Delivery.PacketID = packetID
	pending.BrokerMeta.Delivery.InflightDirection = byte(messages.Outbound)
	pending.BrokerMeta.Delivery.InflightState = byte(messages.StatePublishSent)
	inflightKey := fmt.Sprintf("%s%s%d/%d", clientID, inflightPrefix, messages.Outbound, packetID)
	require.NoError(t, store.Messages().Store(inflightKey, pending))
	message.Release(pending)

	store.wills.failGet.Store(true)

	s, _, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.Equal(t, 1, s.OfflineQueue().Len(), "the restore moved the queued message into the session")
	require.Len(t, s.Inflight().GetAll(), 1, "the restore moved the inflight message into the session")

	_, err = b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.ErrorIs(t, err, errWillStoreUnavailable)

	b.releaseUnattachedSession(context.Background(), s, claim.epoch)

	restoredQueue, err := store.Messages().List(clientID + queuePrefix)
	require.NoError(t, err)
	require.Len(t, restoredQueue, 1, "queued messages must survive a failed CONNECT")
	require.Equal(t, []byte("queued-payload"), restoredQueue[0].PayloadBytes())

	restoredInflight, err := store.Messages().List(clientID + inflightPrefix)
	require.NoError(t, err)
	require.Len(t, restoredInflight, 1, "inflight messages must survive a failed CONNECT")
	require.Equal(t, uint16(packetID), restoredInflight[0].BrokerMeta.Delivery.PacketID)
}

// A persistent CONNECT reuses the session pointer, so identity alone cannot tell
// a session nothing attached to from one another connection has since claimed.
// Rollback keyed on the stale generation must leave the live session alone.
func TestReleaseUnattachedSessionIgnoresNewerGeneration(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	const clientID = "generation-guard"
	s, _, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)

	// A second CONNECT wins the race and attaches to the same session pointer.
	winner, _, winnerClaim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.Same(t, s, winner)

	conn := newSyncConn()
	epoch, err := b.attachSession(context.Background(), winner, winnerClaim, conn, session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)
	require.Greater(t, epoch, claim.epoch)

	// The loser rolls back against the generation it observed.
	b.releaseUnattachedSession(context.Background(), s, claim.epoch)

	require.Same(t, s, b.sessionsMap.Get(clientID), "the attached replacement must survive")
	require.True(t, s.IsConnected())
	require.Equal(t, conn, s.Conn())
}

// Clean Start ends the previous session, so nothing it persisted may be
// inherited — not just its Will. Otherwise a later persistent reconnect restores
// subscriptions the client asked to be rid of.
func TestCleanStartPurgesOrphanedDurableState(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "orphaned-durable"
	const filter = "legacy/#"

	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))
	require.NoError(t, store.Subscriptions().Add(&storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      1,
	}))
	require.Nil(t, b.sessionsMap.Get(clientID))

	_, created, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.True(t, created)

	subs, err := store.Subscriptions().GetForClient(clientID)
	require.NoError(t, err)
	require.Empty(t, subs, "Clean Start must not leave the old subscriptions behind")
}

// Expiry ends the session, and a Will waiting on its delay is due when the
// session ends or the delay passes, whichever comes first. The sweep used to
// delete the record without ever publishing it.
func TestExpireSessionPublishesDueWill(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "expiring-will"
	const willTopic = "clients/expiring-will/status"

	sub, _, err := b.CreateSession("expiring-will-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	s, _, err := b.CreateSession(clientID, 5, session.Options{
		ExpiryInterval: 1,
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    willTopic,
			Payload:  []byte(willPayloadOffline),
			Delay:    3600,
		},
	})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)
	require.NoError(t, s.Disconnect(false, v5.DisconnectUnspecifiedError))
	waitFor(t, func() bool {
		_, err := store.Wills().Get(context.Background(), clientID)
		return err == nil
	}, "disconnect callback stores the delayed Will")

	// A deadline in the future stands in for the wait, so the sweep's own expiry
	// check runs for real without the test sleeping through it.
	b.expireSession(context.Background(), s, time.Now().Add(time.Hour))

	require.Nil(t, b.sessionsMap.Get(clientID))
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "an expiring session publishes its delayed Will")
}

// A session a CONNECT has installed but not attached to has never held a
// connection, so it has no disconnect timestamp. Read as one, it looks overdue
// by the whole span since the zero time, and the sweep destroys the session
// together with the durable state it just restored.
func TestExpireSessionsSkipsUnattachedSession(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "unattached-session"
	const filter = "sensors/+/temp"

	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))
	require.NoError(t, store.Subscriptions().Add(&storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      1,
	}))

	s, _, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.False(t, s.IsConnected(), "the CONNECT has not attached yet")

	// The sweep runs between creation and attachment.
	b.expireSessions()

	require.Same(t, s, b.sessionsMap.Get(clientID), "an unattached session has not expired")
	stored, err := store.Sessions().Get(clientID)
	require.NoError(t, err)
	require.NotNil(t, stored, "the restored durable session must survive the sweep")
	subs, err := store.Subscriptions().GetForClient(clientID)
	require.NoError(t, err)
	require.Len(t, subs, 1)

	_, err = b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)
}

// The sweep reads the expiry interval while the CONNECT it belongs to is still
// setting it, so the read has to share the session's lock.
func TestExpireSessionsRacesAttachingSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	const clientID = "expiry-race"
	s, _, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)

	var sweeps sync.WaitGroup
	sweeps.Add(1)
	go func() {
		defer sweeps.Done()
		for range 100 {
			b.expireSessions()
		}
	}()

	interval := uint32(600)
	_, err = b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, &interval)
	require.NoError(t, err)
	sweeps.Wait()

	require.Same(t, s, b.sessionsMap.Get(clientID))
	require.True(t, s.IsConnected())
}

// The sweep picks candidates without the client-ID lock, so a reconnect can land
// between the scan and the retirement. Destroying whatever answers to the client
// ID by then takes out a live connection.
func TestExpireSessionSkipsReconnectedSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	const clientID = "reconnecting-client"
	s, _, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 1})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)
	require.NoError(t, s.Disconnect(false, v5.DisconnectUnspecifiedError))

	// The scan observed an expired disconnected session; the client reconnects
	// before the sweep takes the lock.
	conn := newSyncConn()
	_, err = s.Connect(conn)
	require.NoError(t, err)

	b.expireSession(context.Background(), s, time.Now().Add(time.Hour))

	require.Same(t, s, b.sessionsMap.Get(clientID), "a reconnected session must survive the sweep")
	require.True(t, s.IsConnected())
}

// The same race with a Clean Start replacement: the client ID now names a
// different session, and the sweep must not destroy it on the old one's behalf.
func TestExpireSessionSkipsReplacedSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	const clientID = "replaced-client"
	retired, _, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 1})
	require.NoError(t, err)
	_, err = retired.Connect(newSyncConn())
	require.NoError(t, err)
	require.NoError(t, retired.Disconnect(false, v5.DisconnectUnspecifiedError))

	replacement, _, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.NotSame(t, retired, replacement)
	_, err = replacement.Connect(newSyncConn())
	require.NoError(t, err)

	b.expireSession(context.Background(), retired, time.Now().Add(time.Hour))

	require.Same(t, replacement, b.sessionsMap.Get(clientID), "the replacement must survive the sweep")
	require.True(t, replacement.IsConnected())
}

// Cancelling a delayed Will is reserved for "a new Network Connection to this
// Session" [MQTT-3.1.3-9]. A CleanStart=false CONNECT that finds no persisted
// session starts a fresh one, so the session that armed the Will has ended and
// the Will is due now [MQTT-3.1.2-8]. Merely declining to cancel it is not
// enough: the delay sweep drops a pending Will once a session is connected
// under that client ID, so it is never published.
func TestFreshSessionPublishesOrphanedWill(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "fresh-session-orphan"
	const willTopic = "clients/fresh-session-orphan/status"

	sub, _, err := b.CreateSession("fresh-session-orphan-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	require.NoError(t, store.Wills().Set(context.Background(), clientID, &storage.WillMessage{
		ClientID: clientID,
		Topic:    willTopic,
		Payload:  []byte(willPayloadOffline),
		Delay:    3600,
	}))

	s, created, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, claim.continuesSession, "no persisted session means a fresh one")

	_, err = b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)

	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "a fresh session publishes the previous session's orphaned Will")

	_, err = store.Wills().Get(context.Background(), clientID)
	require.ErrorIs(t, err, storage.ErrNotFound, "the claimed Will must not be publishable twice")
}

// orphanSubscriptionCluster holds cluster routing entries for a client ID whose
// session is gone from this node, and counts the writes made to clear them.
type orphanSubscriptionCluster struct {
	cluster.Cluster
	mu       sync.Mutex
	subs     []*storage.Subscription
	removals int
}

func (c *orphanSubscriptionCluster) NodeID() string { return testNodeID }

func (c *orphanSubscriptionCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (c *orphanSubscriptionCluster) AcquireSession(context.Context, string, string) error {
	return nil
}

func (c *orphanSubscriptionCluster) ReleaseSession(context.Context, string) error { return nil }

func (c *orphanSubscriptionCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.subs, nil
}

func (c *orphanSubscriptionCluster) RemoveAllSubscriptions(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = nil
	c.removals++

	return nil
}

func (c *orphanSubscriptionCluster) snapshot() (subs []*storage.Subscription, removals int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.subs, c.removals
}

// Cluster routing entries outlive the local records written beside them, and a
// persistent restore reads subscriptions from the cluster in preference to local
// storage. An entry left behind is inherited by the next session under that
// client ID, whoever it now belongs to.
func TestFreshSessionPurgesOrphanedClusterSubscriptions(t *testing.T) {
	const clientID = "orphan-cluster-subs"
	const filter = "legacy/#"

	cl := &orphanSubscriptionCluster{
		subs: []*storage.Subscription{{ClientID: clientID, Filter: filter, QoS: 1}},
	}
	b := NewBroker(memory.New(), cl)
	defer b.Close()

	s, created, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, s.GetSubscriptions(), "a fresh session must not inherit the orphaned cluster subscriptions")

	subs, removals := cl.snapshot()
	require.Empty(t, subs, "the orphaned cluster subscriptions must be removed")
	require.Equal(t, 1, removals)
}

// The counterpart: nothing to detect means no write. An unconditional removal
// would put an etcd round trip on every cold CONNECT.
func TestFreshSessionSkipsClusterRemovalWithoutOrphans(t *testing.T) {
	cl := &orphanSubscriptionCluster{}
	b := NewBroker(memory.New(), cl)
	defer b.Close()

	_, created, err := b.CreateSession("cold-connect", 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.True(t, created)

	_, removals := cl.snapshot()
	require.Zero(t, removals, "a cold CONNECT must not write to the cluster routing table")
}

// A client ID's key spaces expire independently — badger gives the session
// record a TTL of its own expiry interval while the subscriptions and messages
// keyed by the same client ID stay — so a missing session record says nothing
// about what else is left. A fresh session must inherit none of it.
func TestFreshSessionPurgesOrphanedStateWithoutSessionRecord(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "orphan-no-record"
	const filter = "legacy/#"

	require.NoError(t, store.Subscriptions().Add(&storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      1,
	}))
	queued := message.New("legacy/reading", []byte("stale"))
	require.NoError(t, store.Messages().Store(clientID+queuePrefix+"0", queued))
	message.Release(queued)

	stored, err := store.Sessions().Get(clientID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Nil(t, stored, "the case under test has no session record")

	s, created, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, s.GetSubscriptions(), "a fresh session must not inherit the old subscriptions")
	require.Equal(t, 0, s.OfflineQueue().Len(), "a fresh session must not inherit the old queued messages")

	subs, err := store.Subscriptions().GetForClient(clientID)
	require.NoError(t, err)
	require.Empty(t, subs, "the orphaned subscriptions must be purged")

	msgs, err := store.Messages().List(clientID + queuePrefix)
	require.NoError(t, err)
	require.Empty(t, msgs, "the orphaned queued messages must be purged")
}

// The counterpart: a reconnect that genuinely resumes a persisted session is
// "this Session", so it does cancel the pending Will.
func TestResumedSessionCancelsPendingWill(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "resumed-session"
	require.NoError(t, store.Sessions().Save(&storage.Session{
		ClientID:       clientID,
		Version:        5,
		ExpiryInterval: 300,
	}))
	require.NoError(t, store.Wills().Set(context.Background(), clientID, &storage.WillMessage{
		ClientID: clientID,
		Topic:    "clients/resumed-session/status",
		Payload:  []byte(willPayloadOffline),
		Delay:    3600,
	}))

	s, _, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.True(t, claim.continuesSession, "a persisted record continues the session")

	_, err = b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)

	_, err = store.Wills().Get(context.Background(), clientID)
	require.ErrorIs(t, err, storage.ErrNotFound)
}
