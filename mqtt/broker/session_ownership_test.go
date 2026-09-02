// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

type ownershipFailCluster struct {
	cluster.Cluster
	err error
}

func (c *ownershipFailCluster) NodeID() string { return testNodeID }

func (c *ownershipFailCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (c *ownershipFailCluster) AcquireSession(context.Context, string, string) error {
	return c.err
}

func (c *ownershipFailCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	return nil, nil
}

func TestCreateSessionRejectsFailedOwnershipClaim(t *testing.T) {
	want := errors.New("ownership conflict")
	b := NewBroker(memory.New(), &ownershipFailCluster{err: want})
	t.Cleanup(func() { _ = b.Close() })

	got, created, err := b.CreateSession("contended", 5, session.Options{CleanStart: true})
	require.ErrorIs(t, err, want)
	require.Nil(t, got)
	require.False(t, created)
	require.Nil(t, b.Get("contended"), "an unowned session must not become locally visible")
}

// ShareGroupMembers reports no share group members on other nodes. This stub
// embeds the bare cluster.Cluster interface, so every method it does not define
// is a nil call, and a publish asks the cluster for share group members.
func (*ownershipFailCluster) ShareGroupMembers(_ context.Context, _ string, dst []cluster.ShareMember) ([]cluster.ShareMember, error) {
	return dst, nil
}

func (*ownershipFailCluster) RoutePublishToClient(context.Context, string, string, *message.Envelope) error {
	return cluster.ErrClusterNotEnabled
}
