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

func TestCluster_3NodeFormation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	// Wait for cluster to be ready
	err = cluster.WaitForClusterReady(30 * time.Second)
	require.NoError(t, err)

	// Verify all nodes joined
	node0 := cluster.GetNodeByIndex(0)
	require.NotNil(t, node0)

	nodes := node0.Cluster.Nodes()
	assert.Len(t, nodes, 3)

	t.Log("3-node cluster formed successfully")
}

func TestCluster_LeaderElection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	// Wait for cluster to be ready
	err = cluster.WaitForClusterReady(30 * time.Second)
	require.NoError(t, err)

	// Wait for leader election
	leader, err := cluster.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	require.NotNil(t, leader)

	t.Logf("Leader elected: %s", leader.ID)

	// Verify only one leader
	leaderCount := 0
	for _, node := range cluster.Nodes {
		if node.Cluster.IsLeader() {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "expected exactly one leader")
}

func TestCluster_BasicPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	err = cluster.WaitForClusterReady(30 * time.Second)
	require.NoError(t, err)

	// Connect subscriber to node-0
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(t, node0, "subscriber-1")
	err = subscriber.Connect(true)
	require.NoError(t, err)
	defer subscriber.Disconnect()

	// Subscribe to topic
	err = subscriber.Subscribe("test/topic", 0)
	require.NoError(t, err)

	// Wait for subscription to propagate
	time.Sleep(500 * time.Millisecond)

	// Connect publisher to same node
	publisher := testutil.NewTestMQTTClient(t, node0, "publisher-1")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect()

	// Publish message
	payload := []byte("hello from node-0")
	err = publisher.Publish("test/topic", 0, payload, false)
	require.NoError(t, err)

	// Wait for message
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "test/topic", msg.Topic)
	assert.Equal(t, payload, msg.Payload)

	t.Log("Basic pub/sub successful on single node")
}

func TestCluster_CrossNodePubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	err = cluster.WaitForClusterReady(30 * time.Second)
	require.NoError(t, err)

	// Connect subscriber to node-0
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(t, node0, "subscriber-cross")
	err = subscriber.Connect(true)
	require.NoError(t, err)
	defer subscriber.Disconnect()

	// Subscribe to topic
	err = subscriber.Subscribe("cross/test", 0)
	require.NoError(t, err)

	// Wait for subscription to propagate to all nodes
	time.Sleep(1 * time.Second)

	// Connect publisher to node-1 (different node)
	node1 := cluster.GetNodeByIndex(1)
	publisher := testutil.NewTestMQTTClient(t, node1, "publisher-cross")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect()

	// Publish message from different node
	payload := []byte("cross-node message")
	err = publisher.Publish("cross/test", 0, payload, false)
	require.NoError(t, err)

	// Wait for message
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "cross/test", msg.Topic)
	assert.Equal(t, payload, msg.Payload)

	t.Log("Cross-node pub/sub successful")
}
