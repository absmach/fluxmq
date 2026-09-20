// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A single node runs on NoopCluster, not on a nil cluster, and its
// subscription lookups answer ErrClusterNotEnabled. The session restore path
// has to read that as "there is no routing table here" and fall back to local
// storage: taking it for a failure refuses every CONNECT that resumes a
// session, which is all of persistent sessions on a single-node broker.
func TestPersistentConnectWithoutClustering(t *testing.T) {
	const filter = "single-node/+/telemetry"

	t.Run("fresh client ID", func(t *testing.T) {
		b := NewBroker(memory.New(), cluster.NewNoopCluster("single-node"))
		defer b.Close()

		s, created, _, err := b.createSessionForConnection("no-prior-state", 5, session.Options{
			ExpiryInterval: 300,
		}, false)
		require.NoError(t, err)
		require.True(t, created)
		require.NotNil(t, s)
	})

	t.Run("subscriptions restored from local storage", func(t *testing.T) {
		const clientID = "restarted-client"
		store := memory.New()

		before := NewBroker(store, cluster.NewNoopCluster("single-node"))
		s, _, _, err := before.createSessionForConnection(clientID, 5, session.Options{
			ExpiryInterval: 300,
		}, false)
		require.NoError(t, err)
		require.NoError(t, before.subscribe(s, filter, 1, storage.SubscribeOptions{}))
		before.Close()

		// The same store behind a new broker is what a restart leaves: the
		// subscriptions are in storage and nothing is in memory.
		after := NewBroker(store, cluster.NewNoopCluster("single-node"))
		defer after.Close()

		restored, _, _, err := after.createSessionForConnection(clientID, 5, session.Options{
			ExpiryInterval: 300,
		}, false)
		require.NoError(t, err)
		require.Contains(t, restored.GetSubscriptions(), filter)

		matched, err := after.router.Match("single-node/dev-1/telemetry")
		require.NoError(t, err)
		require.Len(t, matched, 1)
		assert.Equal(t, clientID, matched[0].ClientID)
	})

	t.Run("clean start is unaffected", func(t *testing.T) {
		b := NewBroker(memory.New(), cluster.NewNoopCluster("single-node"))
		defer b.Close()

		_, created, _, err := b.createSessionForConnection("clean-start-client", 5, session.Options{
			CleanStart: true,
		}, false)
		require.NoError(t, err)
		require.True(t, created)
	})
}

// A cluster read that fails for any other reason is still a failed CONNECT:
// on a clustered node the routing table is the record, and guessing from local
// storage would hand the session a subscription set another node has moved on
// from.
func TestSessionSubscriptionsPropagatesClusterFailure(t *testing.T) {
	b := NewBroker(memory.New(), &failingSubscriptionCluster{
		Cluster: cluster.NewNoopCluster("clustered"),
		err:     assert.AnError,
	})
	defer b.Close()

	_, err := b.sessionSubscriptions(context.Background(), "client")
	require.ErrorIs(t, err, assert.AnError)
}

// failingSubscriptionCluster is a cluster whose subscription lookup fails with
// something other than ErrClusterNotEnabled.
type failingSubscriptionCluster struct {
	cluster.Cluster
	err error
}

func (c *failingSubscriptionCluster) GetSubscriptionsForClient(_ context.Context, _ string) ([]*storage.Subscription, error) {
	return nil, c.err
}
