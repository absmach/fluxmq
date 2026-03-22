// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCluster(t *testing.T) *testutil.TestCluster {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	t.Cleanup(cluster.Stop)

	err := cluster.Start()
	require.NoError(t, err)

	err = cluster.WaitForClusterReady(60 * time.Second)
	require.NoError(t, err)

	_, err = cluster.WaitForLeader(30 * time.Second)
	require.NoError(t, err)

	return cluster
}

func TestCluster_LeaderFailover_NewLeaderElected(t *testing.T) {
	cluster := setupCluster(t)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)
	killedID := leader.ID
	t.Logf("Killing leader: %s", killedID)

	err := cluster.KillNode(killedID)
	require.NoError(t, err)

	newLeader, err := cluster.WaitForNewLeader(30*time.Second, killedID)
	require.NoError(t, err)
	assert.NotEqual(t, killedID, newLeader.ID)

	// Verify exactly 1 leader among survivors.
	leaderCount := 0
	for _, node := range cluster.Nodes {
		if node.ID == killedID {
			continue
		}
		if node.Cluster != nil && node.Cluster.IsLeader() {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "expected exactly one leader among survivors")
}

func TestCluster_LeaderFailover_PubSubContinues(t *testing.T) {
	cluster := setupCluster(t)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Pick two non-leader nodes for subscriber and publisher.
	var subNode, pubNode *testutil.TestNode
	for _, node := range cluster.Nodes {
		if node.ID == leader.ID {
			continue
		}
		if subNode == nil {
			subNode = node
		} else {
			pubNode = node
			break
		}
	}
	require.NotNil(t, subNode)
	require.NotNil(t, pubNode)

	subscriber := testutil.NewTestMQTTClient(t, subNode, "failover-sub")
	err := subscriber.Connect(true)
	require.NoError(t, err)
	defer subscriber.Disconnect() //nolint:errcheck // test cleanup

	err = subscriber.Subscribe("failover/topic", 0)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	// Kill leader.
	killedID := leader.ID
	t.Logf("Killing leader: %s", killedID)
	err = cluster.KillNode(killedID)
	require.NoError(t, err)

	_, err = cluster.WaitForNewLeader(30*time.Second, killedID)
	require.NoError(t, err)

	// Allow cluster to stabilize after election.
	time.Sleep(2 * time.Second)

	publisher := testutil.NewTestMQTTClient(t, pubNode, "failover-pub")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect() //nolint:errcheck // test cleanup

	payload := []byte("message after leader failover")
	err = publisher.Publish("failover/topic", 0, payload, false)
	require.NoError(t, err)

	msg, err := subscriber.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "failover/topic", msg.Topic)
	assert.Equal(t, payload, msg.Payload)
}

func TestCluster_NonLeaderCrash_TrafficContinues(t *testing.T) {
	cluster := setupCluster(t)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Find the two non-leader nodes.
	var nonLeaders []*testutil.TestNode
	for _, node := range cluster.Nodes {
		if node.ID != leader.ID {
			nonLeaders = append(nonLeaders, node)
		}
	}
	require.Len(t, nonLeaders, 2)

	// Subscriber on follower A, publisher on leader, kill follower B.
	followerA := nonLeaders[0]
	followerB := nonLeaders[1]

	subscriber := testutil.NewTestMQTTClient(t, followerA, "crash-sub")
	err := subscriber.Connect(true)
	require.NoError(t, err)
	defer subscriber.Disconnect() //nolint:errcheck // test cleanup

	err = subscriber.Subscribe("crash/topic", 0)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	publisher := testutil.NewTestMQTTClient(t, leader, "crash-pub")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect() //nolint:errcheck // test cleanup

	// Kill follower B — neither client is connected to it.
	t.Logf("Killing non-leader: %s", followerB.ID)
	err = cluster.KillNode(followerB.ID)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	payload := []byte("message after non-leader crash")
	err = publisher.Publish("crash/topic", 0, payload, false)
	require.NoError(t, err)

	msg, err := subscriber.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "crash/topic", msg.Topic)
	assert.Equal(t, payload, msg.Payload)

	// Leader should be unchanged.
	assert.True(t, leader.Cluster.IsLeader(), "leader should remain unchanged")
}

func TestCluster_SessionTakeover_AfterNodeCrash(t *testing.T) {
	cluster := setupCluster(t)

	node0 := cluster.GetNodeByIndex(0)
	node1 := cluster.GetNodeByIndex(1)

	client := testutil.NewTestMQTTClient(t, node0, "crash-session-client")
	err := client.Connect(false) // persistent session
	require.NoError(t, err)

	err = client.Subscribe("crash/session", 0)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Kill the entire node the client is connected to.
	t.Logf("Killing node: %s", node0.ID)
	err = cluster.KillNode(node0.ID)
	require.NoError(t, err)

	// Wait for session lease to expire (etcd lease TTL ~30s).
	t.Log("Waiting for session lease expiry...")
	time.Sleep(35 * time.Second)

	// Reconnect to a surviving node.
	err = client.Reconnect(node1)
	require.NoError(t, err)

	// Re-subscribe since we're reconnecting after a full node crash.
	err = client.Subscribe("crash/session", 0)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify message delivery works on the new node.
	publisher := testutil.NewTestMQTTClient(t, node1, "crash-session-pub")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect() //nolint:errcheck // test cleanup

	payload := []byte("post-crash message")
	err = publisher.Publish("crash/session", 0, payload, false)
	require.NoError(t, err)

	msg, err := client.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "crash/session", msg.Topic)
	assert.Equal(t, payload, msg.Payload)

	client.Disconnect() //nolint:errcheck // test cleanup
}

func TestCluster_CrossNodeDelivery_AfterNodeKill(t *testing.T) {
	cluster := setupCluster(t)

	node0 := cluster.GetNodeByIndex(0)
	node1 := cluster.GetNodeByIndex(1)
	node2 := cluster.GetNodeByIndex(2)

	subscriber := testutil.NewTestMQTTClient(t, node0, "xnode-sub")
	err := subscriber.Connect(true)
	require.NoError(t, err)
	defer subscriber.Disconnect() //nolint:errcheck // test cleanup

	err = subscriber.Subscribe("xnode/topic", 0)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	// Kill node-2 (neither subscriber nor publisher uses it).
	t.Logf("Killing node: %s", node2.ID)
	err = cluster.KillNode(node2.ID)
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	publisher := testutil.NewTestMQTTClient(t, node1, "xnode-pub")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect() //nolint:errcheck // test cleanup

	payload := []byte("cross-node after kill")
	err = publisher.Publish("xnode/topic", 0, payload, false)
	require.NoError(t, err)

	msg, err := subscriber.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "xnode/topic", msg.Topic)
	assert.Equal(t, payload, msg.Payload)
}
