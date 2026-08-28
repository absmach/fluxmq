// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
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

	b.expireSession(context.Background(), clientID)

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

// Cancelling a delayed Will is reserved for "a new Network Connection to this
// Session" [MQTT-3.1.3-9]. A CleanStart=false CONNECT that finds no persisted
// session starts a fresh one, so the orphaned Will stays due.
func TestFreshSessionDoesNotCancelOrphanedWill(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "fresh-session-orphan"
	orphan := &storage.WillMessage{
		ClientID: clientID,
		Topic:    "clients/fresh-session-orphan/status",
		Payload:  []byte(willPayloadOffline),
		Delay:    3600,
	}
	require.NoError(t, store.Wills().Set(context.Background(), clientID, orphan))

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

	kept, err := store.Wills().Get(context.Background(), clientID)
	require.NoError(t, err)
	require.NotNil(t, kept, "a fresh session must not cancel the previous session's Will")
	require.Equal(t, orphan.Topic, kept.Topic)
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
