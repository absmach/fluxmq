// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sharedFilter = "$share/workers/sensors/#"
	sharedTopic  = "sensors/room1/temp"
)

// startSharedCluster brings up a three node cluster ready for publishing.
func startSharedCluster(t *testing.T) *testutil.TestCluster {
	t.Helper()

	cluster := testutil.NewTestCluster(t, 3)
	t.Cleanup(cluster.Stop)

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	return cluster
}

// joinShareGroup connects one share group member to the given node.
func joinShareGroup(t *testing.T, node *testutil.TestNode, clientID string) *testutil.TestMQTTClient {
	t.Helper()

	client := testutil.NewTestMQTTClient(t, node, clientID)
	require.NoError(t, client.Connect(true))
	t.Cleanup(func() { client.Disconnect() }) //nolint:errcheck // test cleanup
	require.NoError(t, client.Subscribe(sharedFilter, 1))

	return client
}

// waitForRemoteMembers blocks until node sees want share group members on other
// nodes. Membership travels through etcd, so publishing before it lands would
// test the propagation delay rather than the distribution.
func waitForRemoteMembers(t *testing.T, node *testutil.TestNode, want int) {
	t.Helper()

	require.Eventually(t, func() bool {
		members, err := node.Cluster.ShareGroupMembers(context.Background(), sharedTopic, nil)

		return err == nil && len(members) == want
	}, 30*time.Second, 100*time.Millisecond, "share group membership did not reach the publishing node")
}

func totalReceived(clients ...*testutil.TestMQTTClient) int {
	total := 0
	for _, client := range clients {
		total += client.Messages().Count()
	}

	return total
}

// publishShared sends count messages to the shared topic from node.
func publishShared(t *testing.T, node *testutil.TestNode, clientID string, count int) {
	t.Helper()

	publisher := testutil.NewTestMQTTClient(t, node, clientID)
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect() //nolint:errcheck // test cleanup

	for i := range count {
		require.NoError(t, publisher.Publish(sharedTopic, 1, []byte(fmt.Sprintf("m%d", i)), false))
	}
}

// TestCrossNode_SharedSubscription_DeliversOneCopyPerMessage is the property the
// whole feature rests on, proved against real nodes rather than a stub: members
// spread over three brokers are one group, so a published message arrives once
// in total, not once per node holding a member.
func TestCrossNode_SharedSubscription_DeliversOneCopyPerMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := startSharedCluster(t)

	members := make([]*testutil.TestMQTTClient, 3)
	for i := range members {
		members[i] = joinShareGroup(t, cluster.GetNodeByIndex(i), fmt.Sprintf("worker-%d", i))
	}

	publisherNode := cluster.GetNodeByIndex(0)
	waitForRemoteMembers(t, publisherNode, 2)

	const messages = 30
	publishShared(t, publisherNode, "shared-publisher", messages)

	require.Eventually(t, func() bool {
		return totalReceived(members...) >= messages
	}, 30*time.Second, 100*time.Millisecond, "the group did not receive every message")

	// A duplicate would arrive after the count is already complete, so the
	// absence of one has to be waited for rather than sampled.
	require.Never(t, func() bool {
		return totalReceived(members...) > messages
	}, 3*time.Second, 200*time.Millisecond, "a message reached more than one member")

	for i, member := range members {
		assert.NotZero(t, member.Messages().Count(),
			"worker-%d took no share of the group's work", i)
	}
}

// TestCrossNode_SharedSubscription_GroupWithNoLocalMember covers the case the
// publishing node's own router cannot see: every member is connected elsewhere,
// so nothing local matches the topic at all.
func TestCrossNode_SharedSubscription_GroupWithNoLocalMember(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := startSharedCluster(t)

	remote := []*testutil.TestMQTTClient{
		joinShareGroup(t, cluster.GetNodeByIndex(1), "worker-1"),
		joinShareGroup(t, cluster.GetNodeByIndex(2), "worker-2"),
	}

	publisherNode := cluster.GetNodeByIndex(0)
	waitForRemoteMembers(t, publisherNode, 2)

	const messages = 20
	publishShared(t, publisherNode, "remote-only-publisher", messages)

	require.Eventually(t, func() bool {
		return totalReceived(remote...) >= messages
	}, 30*time.Second, 100*time.Millisecond, "a group with no local member received nothing")

	require.Never(t, func() bool {
		return totalReceived(remote...) > messages
	}, 3*time.Second, 200*time.Millisecond, "a message reached more than one member")
}

// TestCrossNode_SharedSubscription_SurvivesMemberDisconnect checks that a member
// leaving costs the group nothing. Its subscription outlives its connection
// briefly, so the rotation keeps choosing it until the cluster catches up, and
// every one of those turns has to fall through to a member that is still there.
func TestCrossNode_SharedSubscription_SurvivesMemberDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := startSharedCluster(t)

	staying := joinShareGroup(t, cluster.GetNodeByIndex(1), "worker-staying")
	leaving := joinShareGroup(t, cluster.GetNodeByIndex(2), "worker-leaving")

	publisherNode := cluster.GetNodeByIndex(0)
	waitForRemoteMembers(t, publisherNode, 2)

	require.NoError(t, leaving.Disconnect())

	const messages = 20
	publishShared(t, publisherNode, "disconnect-publisher", messages)

	require.Eventually(t, func() bool {
		return staying.Messages().Count() >= messages
	}, 30*time.Second, 100*time.Millisecond,
		"the remaining member did not receive the messages the departed member's turns should have fallen through to")
}
