// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

type identityRejectingCluster struct {
	cluster.Cluster
	identity     *cluster.SessionIdentityGuard
	acquireCalls int
}

func (c *identityRejectingCluster) NodeID() string { return "node-new" }

func (c *identityRejectingCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return "node-old", true, nil
}

func (c *identityRejectingCluster) TakeoverSession(_ context.Context, _, _, _ string, identity *cluster.SessionIdentityGuard) (*clusterv1.SessionState, error) {
	if identity != nil {
		copy := *identity
		c.identity = &copy
	}
	return nil, cluster.ErrSessionIdentityMismatch
}

func (c *identityRejectingCluster) AcquireSession(context.Context, string, string) error {
	c.acquireCalls++
	return nil
}

func TestSessionTakeoverIdentityMismatchLeavesOwnerUntouched(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	s, _, err := b.CreateSession("bound-client", 5, session.Options{
		ExternalID:     mtlsEntityA,
		CleanStart:     false,
		ExpiryInterval: 300,
	})
	require.NoError(t, err)
	conn := &mockConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)

	state, err := b.GetSessionStateAndClose(context.Background(), s.ID, &cluster.SessionIdentityGuard{
		ExternalID:   mtlsEntityB,
		RequireBound: true,
	})
	require.ErrorIs(t, err, cluster.ErrSessionIdentityMismatch)
	require.Nil(t, state)
	require.Same(t, s, b.Get(s.ID))
	require.True(t, s.IsConnected())
	require.Same(t, conn, s.Conn())
	require.Equal(t, mtlsEntityA, s.ExternalIdentity())
}

func TestSessionTakeoverMatchingIdentityTransfersState(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	hook := &disconnectSpyHook{}
	b.SetEventHook(hook)

	s, _, err := b.CreateSession("bound-client", 5, session.Options{
		ExternalID:     mtlsEntityA,
		CleanStart:     false,
		ExpiryInterval: 300,
	})
	require.NoError(t, err)
	conn := newSyncConn()
	_, err = s.Connect(conn)
	require.NoError(t, err)

	state, err := b.GetSessionStateAndClose(context.Background(), s.ID, &cluster.SessionIdentityGuard{
		ExternalID:   mtlsEntityA,
		RequireBound: true,
	})
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, mtlsEntityA, state.ExternalId)
	require.Nil(t, b.Get(s.ID))
	require.False(t, s.IsConnected())
	waitFor(t, func() bool {
		for _, p := range conn.writtenPackets() {
			if d, ok := p.(*v5.Disconnect); ok && d.ReasonCode == v5.DisconnectSessionTakenOver {
				return true
			}
		}
		return false
	}, "transferred v5 client receives DISCONNECT 0x8E")
	waitFor(t, func() bool { return len(hook.snapshot()) == 1 }, "cross-node retirement emits its disconnect event")
	require.Equal(t, []string{reasonTakeover}, hook.snapshot())
}

func TestMQTTMTLSClusterTakeoverPropagatesIdentityGuard(t *testing.T) {
	for _, version := range []int{4, 5} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			cl := &identityRejectingCluster{}
			b := NewBroker(memory.New(), cl)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
				result: &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityA},
			}, nil))

			err := runMTLSConnect(t, b, version, mqttMTLSContext(t, mtlsEntityA), "bound-client", mtlsEntityA, mtlsAPIKey, true, true, false)
			require.ErrorIs(t, err, ErrNotAuthorized)
			require.Equal(t, &cluster.SessionIdentityGuard{
				ExternalID:   mtlsEntityA,
				RequireBound: true,
			}, cl.identity)
			require.Zero(t, cl.acquireCalls)
			require.Nil(t, b.Get("bound-client"))
		})
	}
}
