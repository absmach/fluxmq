// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexSubscription(t *testing.T) {
	cases := []struct {
		name      string
		filter    string
		wantIndex string
		wantShare string
	}{
		{
			name:      "shared/indexed-under-bare-filter",
			filter:    testShareFilter,
			wantIndex: testTasksFilter,
			wantShare: testShareName,
		},
		{
			name:      "shared/filter-with-one-level",
			filter:    "$share/g/tasks",
			wantIndex: "tasks",
			wantShare: "g",
		},
		{
			name:      "plain/untouched",
			filter:    testTasksFilter,
			wantIndex: testTasksFilter,
		},
		// "$share/x" names no topic filter, so it is not a shared subscription
		// and must not be rewritten into one.
		{
			name:      "malformed/no-topic-filter",
			filter:    "$share/x",
			wantIndex: "$share/x",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := &storage.Subscription{ClientID: "c", Filter: tc.filter}
			assert.Equal(t, tc.wantIndex, indexSubscription(sub))
			assert.Equal(t, tc.wantShare, sub.Options.ShareName)
			assert.Equal(t, tc.filter, sub.Filter, "the stored filter is what the session replays on reconnect")
		})
	}
}

// shareTrieCluster builds a cluster whose subscription trie holds the given
// members, indexed the way the watch would index them.
func shareTrieCluster(t *testing.T, subs []storage.Subscription, owners map[string]string) *EtcdCluster {
	t.Helper()

	c := &EtcdCluster{
		nodeID:     testNodeLocal,
		transport:  &Transport{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		subTrie:    router.NewRouter(),
		ownerCache: map[string]string{},
	}

	var shared int64
	for i := range subs {
		sub := subs[i]
		indexed := indexSubscription(&sub)
		if sub.Options.ShareName != "" {
			shared++
		}
		require.NoError(t, c.subTrie.Subscribe(sub.ClientID, indexed, sub.QoS, sub.Options))
	}
	c.sharedSubCount.Store(shared)

	for clientID, nodeID := range owners {
		c.ownerCache[clientID] = nodeID
	}

	return c
}

func TestShareGroupMembersReportsOnlyRemoteMembers(t *testing.T) {
	c := shareTrieCluster(t,
		[]storage.Subscription{
			{ClientID: testWorkerA, Filter: testShareFilter, QoS: 1},
			{ClientID: testWorkerB, Filter: testShareFilter, QoS: 2},
			{ClientID: "worker-local", Filter: testShareFilter, QoS: 1},
			{ClientID: testPlainA, Filter: testTasksFilter, QoS: 1},
		},
		map[string]string{
			testWorkerA:    testNodeA,
			testWorkerB:    testNodeB,
			"worker-local": testNodeLocal,
			testPlainA:     testNodeA,
		},
	)

	got, err := c.ShareGroupMembers(context.Background(), testTasksTopic, nil)
	require.NoError(t, err)
	require.Len(t, got, 2, "only the members on other nodes are reported")

	byClient := map[string]ShareMember{}
	for _, member := range got {
		byClient[member.ClientID] = member
	}

	assert.Equal(t, ShareMember{ClientID: testWorkerA, NodeID: testNodeA, ShareName: testShareName, Filter: testTasksFilter, QoS: 1}, byClient[testWorkerA])
	assert.Equal(t, ShareMember{ClientID: testWorkerB, NodeID: testNodeB, ShareName: testShareName, Filter: testTasksFilter, QoS: 2}, byClient[testWorkerB])
	assert.NotContains(t, byClient, testPlainA, "an ordinary subscription is not a share group member")
	assert.NotContains(t, byClient, "worker-local", "a local member is the caller's own business")
}

func TestShareGroupMembersSkipsMatchWhenNoneIndexed(t *testing.T) {
	c := shareTrieCluster(t,
		[]storage.Subscription{{ClientID: testPlainA, Filter: testTasksFilter, QoS: 1}},
		map[string]string{testPlainA: testNodeA},
	)
	require.Zero(t, c.sharedSubCount.Load())

	got, err := c.ShareGroupMembers(context.Background(), testTasksTopic, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestRoutePublishSkipsShareGroupMembers guards the rule that keeps a share
// group to one copy per message: the topic broadcast must not carry a message
// to a node merely because a share group member lives there. That member is
// either the one chosen, and sent to directly, or it is not this message's
// turn — and a broadcast cannot tell the difference.
func TestRoutePublishSkipsShareGroupMembers(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	c := shareTrieCluster(t,
		[]storage.Subscription{
			{ClientID: testWorkerA, Filter: testShareFilter, QoS: 1},
			{ClientID: "plain-b", Filter: testTasksFilter, QoS: 1},
		},
		map[string]string{testWorkerA: testNodeA, "plain-b": testNodeB},
	)
	c.stopCh = stopCh

	var (
		mu    sync.Mutex
		calls = map[string]int{}
	)
	c.forwardBatcher = newNodeBatcher(
		1, 5*time.Millisecond, 1, stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test-forward",
		func(_ context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
			mu.Lock()
			calls[nodeID] += len(items)
			mu.Unlock()
			return nil
		},
	)

	msg := message.New(testTasksTopic, []byte("work"))
	msg.BrokerMeta.Delivery.QoS = 1
	defer message.Release(msg)

	require.NoError(t, c.RoutePublish(context.Background(), msg))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, calls[testNodeB], "the node holding the ordinary subscription is forwarded to")
	assert.Zero(t, calls[testNodeA], "the node holding only a share group member is not")
}
