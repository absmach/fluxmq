// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
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
	oldConnect.WillMessage = []byte("offline")
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

	oldConnect := v5ConnectWillDelay(clientID, willTopic, []byte("offline"), 60)
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
			Payload:  []byte("offline"),
		},
	})
	require.NoError(t, err)
	_, err = s.Connect(newSyncConn())
	require.NoError(t, err)

	disconnected := make(chan struct{})
	s.SetOnDisconnect(func(s *session.Session, graceful bool) {
		b.handleDisconnect(s, graceful)
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
				return string(pub.Payload) == "offline"
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
			Payload:  []byte("offline"),
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
				return string(pub.Payload) == "offline"
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
	require.Equal(t, []string{"error"}, hook.snapshot())

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
	require.Equal(t, []string{"takeover"}, hook.snapshot())

	newConn.Close()
	oldWG.Wait()
	newWG.Wait()
}

// drainSuperseded owns the notification for a connection it retires, and only
// when the session behind it ended. A takeover that continues the same session
// keeps the client ID connected under the replacement socket and owes nothing.
func TestDrainSuperseded_NotifiesOnlyForEndedSessions(t *testing.T) {
	const clientID = "spy"

	cases := []struct {
		name       string
		superseded *session.Superseded
		want       []string
	}{
		{
			name:       "clean_replacement/retired_connection",
			superseded: &session.Superseded{ClientID: clientID, Conn: newBlockingConn(), SessionEnds: true},
			want:       []string{"takeover"},
		},
		{
			name:       "takeover/session_continues",
			superseded: &session.Superseded{ClientID: clientID, Conn: newBlockingConn(), SessionEnds: false},
			want:       nil,
		},
		{
			name:       "clean_replacement/no_connection",
			superseded: &session.Superseded{ClientID: clientID, SessionEnds: true},
			want:       nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBroker(nil, nil)
			defer b.Close()
			hook := &disconnectSpyHook{}
			b.SetEventHook(hook)

			b.drainSuperseded(context.Background(), tc.superseded)

			require.Equal(t, tc.want, hook.snapshot())
		})
	}
}
