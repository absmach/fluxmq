// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/message"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

const (
	// reasonTakeover is the ClientDisconnected reason a retired connection reports.
	reasonTakeover = "takeover"
	// willPayloadOffline is the Will payload these tests watch for on the wire.
	willPayloadOffline = "offline"
)

type cleanupSpyCluster struct {
	cluster.Cluster
	mu                          sync.Mutex
	owner                       bool
	acquires                    int
	releases                    int
	removeAllSubscriptionsCalls int
}

func (c *cleanupSpyCluster) NodeID() string { return testNodeID }

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

func (c *cleanupSpyCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	return nil, nil
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

	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		_ = h.HandleConnect(context.Background(), newConn, cleanV3Connect(clientID))
	}()

	waitFor(t, func() bool { return oldConn.closed.Load() }, "old clean connection retired")
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

func TestHandleConnect_CleanV3ReplacementPublishesOldWill(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV3Handler(b)

	const clientID = "clean-v3-will"
	const willTopic = "clients/clean-v3-will/status"

	sub, _, err := b.CreateSession("clean-v3-will-sub", 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	oldConnect := cleanV3Connect(clientID)
	oldConnect.WillFlag = true
	oldConnect.WillTopic = willTopic
	oldConnect.WillMessage = []byte(willPayloadOffline)
	oldConn := newSyncConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(context.Background(), oldConn, oldConnect) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old clean v3 session connected")

	newConn := newSyncConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(context.Background(), newConn, cleanV3Connect(clientID)) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "replacement clean v3 session current")
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v3.Publish); ok && pub.TopicName == willTopic {
				return true
			}
		}
		return false
	}, "old clean v3 connection's Will published")

	oldWG.Wait()
	newConn.Close()
	newWG.Wait()
}

func TestHandleConnect_CleanV5ReplacementNotifiesAndPublishesDelayedWill(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "clean-v5-will"
	const willTopic = "clients/clean-v5-will/status"

	sub, _, err := b.CreateSession("clean-v5-will-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	oldConnect := v5ConnectWillDelay(clientID, willTopic, []byte(willPayloadOffline), 60)
	oldConnect.CleanStart = true
	oldConn := newSyncConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(context.Background(), oldConn, oldConnect) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old clean v5 session connected")

	newConnect := v5Connect(clientID, "", nil)
	newConnect.CleanStart = true
	newConn := newSyncConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(context.Background(), newConn, newConnect) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "replacement clean v5 session current")
	waitFor(t, func() bool {
		for _, p := range oldConn.writtenPackets() {
			if d, ok := p.(*v5.Disconnect); ok && d.ReasonCode == v5.DisconnectSessionTakenOver {
				return true
			}
		}
		return false
	}, "old clean v5 connection receives DISCONNECT 0x8E")
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return true
			}
		}
		return false
	}, "old clean v5 connection's delayed Will published immediately")

	oldWG.Wait()
	newConn.Close()
	newWG.Wait()
}

func TestHandleDisconnect_CleanSessionPublishesWillBeforeDestroy(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	const clientID = "clean-disconnect-will"
	const willTopic = "clients/clean-disconnect-will/status"

	sub, _, err := b.CreateSession("clean-disconnect-will-sub", 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	s, _, err := b.CreateSession(clientID, 4, session.Options{
		CleanStart: true,
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    willTopic,
			Payload:  []byte(willPayloadOffline),
		},
	})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)

	disconnected := make(chan struct{})
	s.SetOnDisconnectWithCause(func(s *session.Session, cause session.DisconnectCause, epoch uint64) {
		b.handleDisconnect(s, cause, epoch)
		close(disconnected)
	})
	require.NoError(t, s.Disconnect(false, v5.DisconnectUnspecifiedError))
	waitFor(t, func() bool {
		select {
		case <-disconnected:
			return true
		default:
			return false
		}
	}, "clean disconnect callback completes")

	require.Nil(t, b.Get(clientID), "clean session is destroyed after its Will is captured")
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v3.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "clean disconnect callback publishes the Will")
}

func TestCreateSession_CleanStartPublishesStoredDelayedWill(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "stored-delayed-will"
	const willTopic = "clients/stored-delayed-will/status"

	sub, _, err := b.CreateSession("stored-delayed-will-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	old, _, err := b.CreateSession(clientID, 5, session.Options{
		ExpiryInterval: 300,
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    willTopic,
			Payload:  []byte(willPayloadOffline),
			Delay:    60,
		},
	})
	require.NoError(t, err)
	_, err = old.Connect(newSyncConn())
	require.NoError(t, err)
	require.NoError(t, old.Disconnect(false, v5.DisconnectUnspecifiedError))
	waitFor(t, func() bool {
		_, err := store.Wills().Get(context.Background(), clientID)
		return err == nil
	}, "disconnect callback stores the delayed Will")

	replacement, created, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.True(t, created)
	require.NotSame(t, old, replacement)
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "Clean Start publishes the stored delayed Will immediately")
}

func TestHandleDisconnect_StaleCleanSessionDoesNotDeleteReplacementClusterState(t *testing.T) {
	cl := &cleanupSpyCluster{}
	b := NewBroker(nil, cl)
	defer b.Close()

	const clientID = "cluster-clean-reconnect"
	oldSession, _, err := b.CreateSession(clientID, 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	oldEpoch := oldSession.Epoch()

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
	b.handleDisconnect(oldSession, session.DisconnectAbnormal, oldEpoch)

	require.Same(t, replacement, b.sessionsMap.Get(clientID))
	owner, acquires, releases, removeAllSubscriptionsCalls = cl.snapshot()
	require.True(t, owner)
	require.Equal(t, 2, acquires)
	require.Equal(t, 1, releases)
	require.Equal(t, 1, removeAllSubscriptionsCalls)
}

func TestHandleDisconnect_StalePersistentCallbackCannotMutateReplacementGeneration(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()
	hook := &disconnectSpyHook{}
	b.SetEventHook(hook)

	const clientID = "persistent-stale-callback"
	oldWill := &storage.WillMessage{
		ClientID: clientID,
		Topic:    "clients/persistent-stale-callback/old",
		Payload:  []byte("old"),
		Delay:    60,
	}
	s, _, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
		Will:           oldWill,
	}, false)
	require.NoError(t, err)
	oldEpoch, err := b.attachSession(context.Background(), s, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		Will:           oldWill,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	s.SetOnDisconnectWithCause(func(s *session.Session, cause session.DisconnectCause, epoch uint64) {
		close(callbackStarted)
		<-releaseCallback
		b.handleDisconnect(s, cause, epoch)
		close(callbackDone)
	})
	require.NoError(t, s.Disconnect(false, v5.DisconnectUnspecifiedError))
	<-callbackStarted

	claimed, created, replacementClaim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.False(t, created)
	require.Same(t, s, claimed, "persistent reconnect must reuse the session pointer")
	require.Equal(t, oldEpoch, replacementClaim.epoch)

	newWill := &storage.WillMessage{
		ClientID: clientID,
		Topic:    "clients/persistent-stale-callback/new",
		Payload:  []byte("new"),
		Delay:    120,
	}
	newEpoch, err := b.attachSession(context.Background(), s, replacementClaim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		Will:           newWill,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)
	require.Greater(t, newEpoch, oldEpoch)

	queued := message.NewDelivery("replacement/queued", []byte("replacement"), 1, false)
	require.NoError(t, s.OfflineQueue().Enqueue(queued))
	message.Release(queued)

	close(releaseCallback)
	<-callbackDone
	s.SetOnDisconnectWithCause(func(s *session.Session, cause session.DisconnectCause, epoch uint64) {
		b.handleDisconnect(s, cause, epoch)
	})

	require.Same(t, newWill, s.GetWill(), "stale callback must not consume the replacement Will")
	require.Equal(t, 1, s.OfflineQueue().Len(), "stale callback must not persist and drain replacement queue state")
	_, err = store.Wills().Get(context.Background(), clientID)
	require.ErrorIs(t, err, storage.ErrNotFound, "stale callback must not persist the replacement Will")
	stored, err := store.Sessions().Get(clientID)
	require.NoError(t, err)
	require.True(t, stored.Connected, "stale callback must not persist the replacement as disconnected")
	require.Equal(t, []string{disconnectReasonError}, hook.snapshot(), "the old physical disconnect is still reported exactly once")
}

func TestAttachSessionRejectsClaimReplacedByCleanStart(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()

	const clientID = "replaced-before-attach"
	stale, _, staleClaim, err := b.createSessionForConnection(clientID, 4, session.Options{}, false)
	require.NoError(t, err)
	replacement, _, err := b.CreateSession(clientID, 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.NotSame(t, stale, replacement)

	_, err = b.attachSession(context.Background(), stale, staleClaim, newSyncConn(), session.ConnectOptions{
		Version:        4,
		ReceiveMaximum: 16,
	}, nil)
	require.ErrorIs(t, err, errSessionReplacedBeforeAttach)
	require.Same(t, replacement, b.Get(clientID))
	require.False(t, stale.IsConnected())
}

func TestHandleDisconnect_PersistentZeroDelayWillPublishesWithoutStorage(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "persistent-zero-delay"
	const willTopic = "clients/persistent-zero-delay/status"
	sub, _, err := b.CreateSession("persistent-zero-delay-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	s, _, err := b.CreateSession(clientID, 5, session.Options{
		ExpiryInterval: 300,
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    willTopic,
			Payload:  []byte(willPayloadOffline),
		},
	})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)
	require.NoError(t, s.Disconnect(false, v5.DisconnectUnspecifiedError))

	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "persistent zero-delay Will is published directly")
	_, err = store.Wills().Get(context.Background(), clientID)
	require.ErrorIs(t, err, storage.ErrNotFound, "zero-delay Wills must never enter delayed-Will storage")
}

func TestHandleDisconnect_AppliesSessionExpiryInterval(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry"
	s, _, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)

	expiry := uint32(120)
	pkt := &v5.Disconnect{Properties: &v5.DisconnectProperties{SessionExpiryInterval: &expiry}}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)
	require.Equal(t, uint32(120), s.Info().ExpiryInterval)
	require.NotNil(t, b.Get(clientID), "non-zero expiry must keep the session alive")

	waitFor(t, func() bool {
		stored, err := store.Sessions().Get(clientID)
		return err == nil && stored.ExpiryInterval == 120
	}, "overridden expiry reaches the stored session record")
}

func TestHandleDisconnect_NilPropertiesKeepsExpiry(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry-nil"
	s, _, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)

	pkt := &v5.Disconnect{}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)
	require.Equal(t, uint32(300), s.Info().ExpiryInterval)
}

func TestHandleDisconnect_ZeroExpiryEndsPersistentSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry-zero"
	s, _, err := b.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)

	expiry := uint32(0)
	pkt := &v5.Disconnect{Properties: &v5.DisconnectProperties{SessionExpiryInterval: &expiry}}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)

	waitFor(t, func() bool { return b.Get(clientID) == nil }, "zero expiry ends the persistent session")
}

func TestHandleDisconnect_NonZeroExpiryAfterConnectZeroIsProtocolError(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry-protocol-error"
	s, _, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	conn := newSyncConn()
	_, err = s.Connect(conn)
	require.NoError(t, err)

	expiry := uint32(3600)
	pkt := &v5.Disconnect{Properties: &v5.DisconnectProperties{SessionExpiryInterval: &expiry}}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)

	waitFor(t, func() bool {
		for _, p := range conn.writtenPackets() {
			if d, ok := p.(*v5.Disconnect); ok && d.ReasonCode == v5.DisconnectProtocolError {
				return true
			}
		}
		return false
	}, "server replies DISCONNECT 0x82 on protocol error")

	waitFor(t, func() bool { return b.Get(clientID) == nil }, "session still ends after protocol error")
}

func TestHandleDisconnect_OverriddenExpirySurvivesReconnect(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry-reconnect"

	connectExpiry := uint32(300)
	connect := v5Connect(clientID, "", nil)
	connect.Properties = &v5.ConnectProperties{SessionExpiryInterval: &connectExpiry}
	conn := newSyncConn()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.HandleConnect(context.Background(), conn, connect) //nolint:errcheck
	}()

	<-conn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "persistent v5 session connected")

	s := b.sessionsMap.Get(clientID)
	require.NotNil(t, s)

	disconnectExpiry := uint32(120)
	pkt := &v5.Disconnect{Properties: &v5.DisconnectProperties{SessionExpiryInterval: &disconnectExpiry}}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)
	wg.Wait()

	waitFor(t, func() bool {
		stored, err := store.Sessions().Get(clientID)
		return err == nil && stored.ExpiryInterval == 120
	}, "overridden expiry reaches the stored session record")

	// The override must leave a session that is still there to resume, not one
	// the disconnect ended.
	reconnect := v5Connect(clientID, "", nil)
	reconnectConn := newSyncConn()
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.HandleConnect(context.Background(), reconnectConn, reconnect) //nolint:errcheck
	}()

	<-reconnectConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "session resumed after the overridden expiry")

	var sawConnAck bool
	for _, p := range reconnectConn.writtenPackets() {
		if ack, ok := p.(*v5.ConnAck); ok {
			sawConnAck = true
			require.True(t, ack.SessionPresent, "reconnect resumes the session the override kept alive")
		}
	}
	require.True(t, sawConnAck, "reconnect is answered with a CONNACK")

	reconnectConn.Close()
	wg.Wait()
}

func TestHandleDisconnect_NonZeroExpiryAfterDefaultedConnectZeroIsProtocolError(t *testing.T) {
	b := NewBroker(memory.New(), nil, WithSessionConfig(config.SessionConfig{DefaultExpiryInterval: 300}))
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry-defaulted"
	s, _, err := b.CreateSession(clientID, 5, session.Options{})
	require.NoError(t, err)
	require.Equal(t, uint32(300), s.Info().ExpiryInterval, "the server default replaces the client's zero")
	conn := newSyncConn()
	_, err = s.Connect(conn)
	require.NoError(t, err)

	expiry := uint32(3600)
	pkt := &v5.Disconnect{Properties: &v5.DisconnectProperties{SessionExpiryInterval: &expiry}}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)

	// The CONNECT carried no expiry, so it carried zero: the server default it
	// was given must not make the override legal.
	waitFor(t, func() bool {
		for _, p := range conn.writtenPackets() {
			if d, ok := p.(*v5.Disconnect); ok && d.ReasonCode == v5.DisconnectProtocolError {
				return true
			}
		}
		return false
	}, "server replies DISCONNECT 0x82 despite the defaulted expiry")

	waitFor(t, func() bool { return b.Get(clientID) == nil }, "session ends on the interval the client asked for")
}

func TestHandleDisconnect_ProtocolErrorPublishesWill(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "disconnect-expiry-protocol-error-will"
	const willTopic = "clients/disconnect-expiry-protocol-error-will/status"

	sub, _, err := b.CreateSession(clientID+"-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	s, _, err := b.CreateSession(clientID, 5, session.Options{
		CleanStart: true,
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    willTopic,
			Payload:  []byte(willPayloadOffline),
		},
	})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)

	expiry := uint32(3600)
	pkt := &v5.Disconnect{Properties: &v5.DisconnectProperties{SessionExpiryInterval: &expiry}}
	require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)

	// The server closes on a Protocol Error, so this is not the clean
	// disconnect that discards a Will.
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "protocol error still publishes the Will")
}

// A migrated session hands over the interval its previous connection
// negotiated. That interval keeps it alive until someone attaches to it, but it
// says nothing about how long the next client wants its session kept: the
// CONNECT that resumes it decides, and [MQTT-3.1.2-11] reads an absent Session
// Expiry Interval as zero.
func TestHandleConnect_MigratedSessionTakesTheConnectExpiry(t *testing.T) {
	for _, tc := range []struct {
		name            string
		connectExemplar *uint32
		wantExpiry      uint32
	}{
		{name: "absent_expiry_is_zero", connectExemplar: nil, wantExpiry: 0},
		{name: "own_expiry_governs", connectExemplar: ptrUint32(600), wantExpiry: 600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := &takeoverCluster{state: &clusterv1.SessionState{ExpiryInterval: 300}}
			b := NewBroker(memory.New(), cl)
			defer b.Close()
			h := newV5Handler(b)

			clientID := "migrated-expiry-" + tc.name
			connect := v5Connect(clientID, "", nil)
			if tc.connectExemplar != nil {
				connect.Properties = &v5.ConnectProperties{SessionExpiryInterval: tc.connectExemplar}
			}

			conn := newSyncConn()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				h.HandleConnect(context.Background(), conn, connect) //nolint:errcheck
			}()

			<-conn.reading
			waitFor(t, func() bool {
				s := b.sessionsMap.Get(clientID)
				return s != nil && s.IsConnected()
			}, "migrated session attached")

			s := b.sessionsMap.Get(clientID)
			require.NotNil(t, s)
			require.Equal(t, tc.wantExpiry, s.Info().ExpiryInterval, "the attaching CONNECT decides how long its session is kept")
			require.Equal(t, tc.wantExpiry, s.ConnectExpiryInterval())

			conn.Close()
			wg.Wait()

			if tc.wantExpiry == 0 {
				waitFor(t, func() bool { return b.Get(clientID) == nil }, "a session with no expiry ends with its connection")
			}
		})
	}
}

func ptrUint32(v uint32) *uint32 { return &v }

// A session restored from storage is continued just as a migrated one is: the
// record the previous connection left behind keeps it until someone attaches,
// and the CONNECT that does decides how long it is kept from then on.
func TestHandleConnect_RestoredSessionTakesTheConnectExpiry(t *testing.T) {
	for _, tc := range []struct {
		name          string
		connectExpiry *uint32
		wantExpiry    uint32
	}{
		{name: "absent_expiry_is_zero", connectExpiry: nil, wantExpiry: 0},
		{name: "own_expiry_governs", connectExpiry: ptrUint32(600), wantExpiry: 600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.New()
			clientID := "restored-expiry-" + tc.name

			// Leave a stored session behind, the way a broker restart does.
			previous := NewBroker(store, nil)
			s, _, err := previous.CreateSession(clientID, 5, session.Options{ExpiryInterval: 300})
			require.NoError(t, err)
			_, err = s.Connect(newSyncConn())
			require.NoError(t, err)
			previous.persistSessionInfo(s)
			require.NoError(t, s.Disconnect(true, v5.DisconnectNormalDisconnection))
			require.NoError(t, previous.Close())

			stored, err := store.Sessions().Get(clientID)
			require.NoError(t, err)
			require.Equal(t, uint32(300), stored.ExpiryInterval, "the previous connection's interval is what is on disk")

			b := NewBroker(store, nil)
			defer b.Close()
			h := newV5Handler(b)

			connect := v5Connect(clientID, "", nil)
			if tc.connectExpiry != nil {
				connect.Properties = &v5.ConnectProperties{SessionExpiryInterval: tc.connectExpiry}
			}
			conn := newSyncConn()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				h.HandleConnect(context.Background(), conn, connect) //nolint:errcheck
			}()

			<-conn.reading
			waitFor(t, func() bool {
				s := b.sessionsMap.Get(clientID)
				return s != nil && s.IsConnected()
			}, "restored session attached")

			restored := b.sessionsMap.Get(clientID)
			require.NotNil(t, restored)
			require.Equal(t, tc.wantExpiry, restored.Info().ExpiryInterval, "the attaching CONNECT decides, not the stored record")
			require.Equal(t, tc.wantExpiry, restored.ConnectExpiryInterval())

			conn.Close()
			wg.Wait()
		})
	}
}

// The Reason Code decides two separate things, and they do not move together:
// whether the Will is published, and whether the connection ended the way the
// protocol intends. 0x04 is an orderly end that asks for the Will.
func TestHandleDisconnect_ReasonCodeDecidesWillAndClassification(t *testing.T) {
	for _, tc := range []struct {
		name        string
		reasonCode  byte
		publishWill bool
		wantReason  string
	}{
		{
			name:        "normal_disconnection",
			reasonCode:  v5.DisconnectNormalDisconnection,
			publishWill: false,
			wantReason:  disconnectReasonNormal,
		},
		{
			name:        "with_will_message",
			reasonCode:  v5.DisconnectDisconnectWithWillMessage,
			publishWill: true,
			wantReason:  disconnectReasonNormal,
		},
		{
			name:        "client_error_code",
			reasonCode:  v5.DisconnectUnspecifiedError,
			publishWill: true,
			wantReason:  disconnectReasonError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBroker(memory.New(), nil)
			defer b.Close()
			hook := &disconnectSpyHook{}
			b.SetEventHook(hook)
			h := newV5Handler(b)

			clientID := "disconnect-reason-" + tc.name
			willTopic := "clients/" + clientID + "/status"

			sub, _, err := b.CreateSession(clientID+"-sub", 5, session.Options{CleanStart: true})
			require.NoError(t, err)
			subConn := newSyncConn()
			_, err = sub.Connect(subConn)
			require.NoError(t, err)
			require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

			s, _, err := b.CreateSession(clientID, 5, session.Options{
				CleanStart: true,
				Will: &storage.WillMessage{
					ClientID: clientID,
					Topic:    willTopic,
					Payload:  []byte(willPayloadOffline),
				},
			})
			require.NoError(t, err)
			_, err = s.Connect(newSyncConn())
			require.NoError(t, err)

			// Waiting on the callback rather than on the Will itself is what
			// lets the negative case assert an absence.
			disconnected := make(chan struct{})
			s.SetOnDisconnectWithCause(func(s *session.Session, cause session.DisconnectCause, epoch uint64) {
				b.handleDisconnect(s, cause, epoch)
				close(disconnected)
			})

			pkt := &v5.Disconnect{ReasonCode: tc.reasonCode}
			require.ErrorIs(t, h.HandleDisconnect(bindConn(s), pkt), io.EOF)
			waitFor(t, func() bool {
				select {
				case <-disconnected:
					return true
				default:
					return false
				}
			}, "disconnect callback completes")

			var published bool
			for _, p := range subConn.writtenPackets() {
				if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
					published = string(pub.Payload) == willPayloadOffline
				}
			}
			// A Will is discarded only on 0x00; 0x04 is the client asking for
			// it to go out. [MQTT-3.1.2-8]
			require.Equal(t, tc.publishWill, published, "Will publication")

			waitFor(t, func() bool { return len(hook.snapshot()) > 0 }, "disconnect reaches the event hook")
			require.Equal(t, []string{tc.wantReason}, hook.snapshot(), "reported disconnect reason")
		})
	}
}

func TestAttachSessionCancelsStoredDelayedWillBeforeLaterCleanStart(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "cancel-stored-delayed-will"
	const willTopic = "clients/cancel-stored-delayed-will/status"
	sub, _, err := b.CreateSession("cancel-stored-delayed-will-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	oldWill := &storage.WillMessage{
		ClientID: clientID,
		Topic:    willTopic,
		Payload:  []byte("obsolete"),
		Delay:    60,
	}
	s, _, err := b.CreateSession(clientID, 5, session.Options{
		ExpiryInterval: 300,
		Will:           oldWill,
	})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)
	require.NoError(t, s.Disconnect(false, v5.DisconnectUnspecifiedError))
	waitFor(t, func() bool {
		_, err := store.Wills().Get(context.Background(), clientID)
		return err == nil
	}, "delayed Will stored after disconnect")

	claimed, created, claim, err := b.createSessionForConnection(clientID, 5, session.Options{
		ExpiryInterval: 300,
	}, false)
	require.NoError(t, err)
	require.False(t, created)
	_, err = b.attachSession(context.Background(), claimed, claim, newSyncConn(), session.ConnectOptions{
		Version:        5,
		KeepAlive:      time.Minute,
		ReceiveMaximum: 16,
	}, nil)
	require.NoError(t, err)
	_, err = store.Wills().Get(context.Background(), clientID)
	require.ErrorIs(t, err, storage.ErrNotFound, "persistent reconnect cancels the previous delayed Will")

	_, _, err = b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.Never(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return true
			}
		}
		return false
	}, 100*time.Millisecond, 5*time.Millisecond, "a later Clean Start must not resurrect a cancelled delayed Will")
}

func TestClaimPendingWillRejectsNewerIdenticalGeneration(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "identical-will-generation"
	will := &storage.WillMessage{
		ClientID: clientID,
		Topic:    "clients/identical-will-generation/status",
		Payload:  []byte(willPayloadOffline),
	}
	ctx := context.Background()
	require.NoError(t, store.Wills().Set(ctx, clientID, will))
	snapshotTime := time.Now()
	pending, err := store.Wills().GetPending(ctx, snapshotTime)
	require.NoError(t, err)
	require.Len(t, pending, 1, "the first generation supplies triggerWills' stale snapshot")

	require.NoError(t, store.Wills().Delete(ctx, clientID))
	require.NoError(t, store.Wills().Set(ctx, clientID, will), "store an identical Will for a newer disconnect generation")

	require.Nil(t, b.claimPendingWill(ctx, clientID, snapshotTime),
		"the old snapshot must not claim an identical newer generation before its deadline")
	current, err := store.Wills().Get(ctx, clientID)
	require.NoError(t, err)
	require.Equal(t, will, current, "rejecting the stale snapshot must preserve the newer generation")
}

// disconnectSpyHook records the reason of every OnDisconnect the broker emits.
type disconnectSpyHook struct {
	mu      sync.Mutex
	reasons []string
}

func (h *disconnectSpyHook) OnConnect(context.Context, string, string, string) error { return nil }

func (h *disconnectSpyHook) OnDisconnect(_ context.Context, _, reason string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reasons = append(h.reasons, reason)

	return nil
}

func (h *disconnectSpyHook) OnSubscribe(context.Context, string, string, byte) error { return nil }

func (h *disconnectSpyHook) OnUnsubscribe(context.Context, string, string) error { return nil }

func (h *disconnectSpyHook) OnPublish(context.Context, string, string, byte, []byte) error {
	return nil
}

func (h *disconnectSpyHook) Close() error { return nil }

func (h *disconnectSpyHook) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.reasons)
}

// A destroyed session's connection is gone, so the disconnect is owed to the
// event hook even though the identity check stops the callback from touching
// state that now belongs to nobody.
func TestDestroySession_EmitsClientDisconnected(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	hook := &disconnectSpyHook{}
	b.SetEventHook(hook)
	h := newV3Handler(b)

	const clientID = "destroy-notifies"
	conn := newBlockingConn()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = h.HandleConnect(context.Background(), conn, v3Connect(clientID))
	}()

	<-conn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "session connected")

	require.NoError(t, b.DestroySession(clientID))

	waitFor(t, func() bool {
		return len(hook.snapshot()) == 1
	}, "destroyed session reports its disconnect")
	require.Equal(t, []string{disconnectReasonError}, hook.snapshot())

	wg.Wait()
}

// A Clean Start replacement retires the old connection without running its
// disconnect callback, so the notification has to come from the retirement path.
func TestHandleConnect_CleanReplacementEmitsClientDisconnected(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	hook := &disconnectSpyHook{}
	b.SetEventHook(hook)
	h := newV3Handler(b)

	const clientID = "clean-replacement-notifies"
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

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "replacement clean session is current")

	waitFor(t, func() bool {
		return len(hook.snapshot()) == 1
	}, "replaced connection reports its disconnect")
	require.Equal(t, []string{reasonTakeover}, hook.snapshot())

	newConn.Close()
	oldWG.Wait()
	newWG.Wait()
}

func TestHandleConnect_PersistentReplacementEmitsOneTakeoverDisconnect(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	hook := &disconnectSpyHook{}
	b.SetEventHook(hook)
	h := newV3Handler(b)

	const clientID = "persistent-replacement-notifies"
	oldConn := newBlockingConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		_ = h.HandleConnect(context.Background(), oldConn, v3Connect(clientID))
	}()
	<-oldConn.reading

	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		_ = h.HandleConnect(context.Background(), newConn, v3Connect(clientID))
	}()
	<-newConn.reading
	waitFor(t, func() bool {
		s := b.Get(clientID)
		return s != nil && s.Conn() == newConn && oldConn.closed.Load()
	}, "persistent replacement retires the old socket")
	oldWG.Wait()

	waitFor(t, func() bool { return len(hook.snapshot()) == 1 }, "persistent takeover reports the retired socket")
	require.Equal(t, []string{reasonTakeover}, hook.snapshot())

	newConn.Close()
	newWG.Wait()
}

// retireSession owns the notification for every physical connection it retires,
// whether the MQTT session ends or continues on another socket.
func TestRetireSession_NotifiesEveryRetiredConnection(t *testing.T) {
	const clientID = "spy"

	cases := []struct {
		name       string
		retirement *sessionRetirement
		want       []string
	}{
		{
			name: "clean_replacement/retired_connection",
			retirement: &sessionRetirement{
				clientID:    clientID,
				superseded:  &session.Superseded{Conn: newBlockingConn()},
				sessionEnds: true,
			},
			want: []string{reasonTakeover},
		},
		{
			name: "takeover/session_continues",
			retirement: &sessionRetirement{
				clientID:   clientID,
				superseded: &session.Superseded{Conn: newBlockingConn()},
			},
			want: []string{reasonTakeover},
		},
		{
			name: "clean_replacement/no_connection",
			retirement: &sessionRetirement{
				clientID:    clientID,
				superseded:  &session.Superseded{},
				sessionEnds: true,
			},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBroker(nil, nil)
			defer b.Close()
			hook := &disconnectSpyHook{}
			b.SetEventHook(hook)

			b.retireSession(context.Background(), tc.retirement)

			require.Equal(t, tc.want, hook.snapshot())
		})
	}
}

// A Clean Start CONNECT ends the previous session even when no session object
// survives to detach it: a restart empties the session map while the records it
// wrote remain. Ending the session makes a pending delayed Will due, so it must
// be published rather than cancelled as if the session had resumed.
//
// A sweep or a lost lease cannot leave this state — the sweep publishes the Will
// and deletes the record (TestExpireSessionPublishesDueWill), and lease loss
// keeps the session in memory.
func TestCreateSession_CleanStartPublishesStoredWillWithoutLocalSession(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	defer b.Close()

	const clientID = "orphaned-delayed-will"
	const willTopic = "clients/orphaned-delayed-will/status"

	sub, _, err := b.CreateSession("orphaned-delayed-will-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	// Model the record a previous broker generation left behind: the Will is in
	// the store, but the session it belonged to is gone from memory.
	require.NoError(t, store.Wills().Set(context.Background(), clientID, &storage.WillMessage{
		ClientID: clientID,
		Topic:    willTopic,
		Payload:  []byte(willPayloadOffline),
		Delay:    60,
	}))
	require.Nil(t, b.sessionsMap.Get(clientID))

	_, created, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	require.True(t, created)

	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return string(pub.Payload) == willPayloadOffline
			}
		}
		return false
	}, "Clean Start publishes a stored Will left without a local session")

	// The claim is exclusive: the record is gone, so neither the sweep nor a
	// later attach can publish or cancel it again.
	_, err = store.Wills().Get(context.Background(), clientID)
	require.ErrorIs(t, err, storage.ErrNotFound)
}

// ShareGroupMembers reports no share group members on other nodes. This stub
// embeds the bare cluster.Cluster interface, so every method it does not define
// is a nil call, and a publish asks the cluster for share group members.
func (*cleanupSpyCluster) ShareGroupMembers(_ context.Context, _ string, dst []cluster.ShareMember) ([]cluster.ShareMember, error) {
	return dst, nil
}

func (*cleanupSpyCluster) RoutePublishToClient(context.Context, string, string, *message.Envelope) error {
	return cluster.ErrClusterNotEnabled
}
