// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
	"time"

	"github.com/absmach/mqtt/testutil"
	"github.com/stretchr/testify/require"
)

func TestSingleNode_BasicStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 1)
	defer cluster.Stop()

	t.Log("Starting single node cluster...")
	err := cluster.Start()
	require.NoError(t, err)

	t.Log("Single node started successfully")

	// Verify node is running
	node0 := cluster.GetNodeByIndex(0)
	require.NotNil(t, node0)
	require.NotNil(t, node0.Broker)
	require.NotNil(t, node0.Cluster)

	t.Log("Single node test passed")
}

func TestSingleNode_BasicPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 1)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	node0 := cluster.GetNodeByIndex(0)

	// Connect subscriber
	subscriber := testutil.NewTestMQTTClient(t, node0, "subscriber-1")
	err = subscriber.Connect(true)
	require.NoError(t, err)
	defer subscriber.Disconnect()

	// Subscribe to topic
	err = subscriber.Subscribe("test/topic", 0)
	require.NoError(t, err)

	// Wait for subscription to register
	time.Sleep(200 * time.Millisecond)

	// Connect publisher
	publisher := testutil.NewTestMQTTClient(t, node0, "publisher-1")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect()

	// Publish message
	payload := []byte("hello world")
	err = publisher.Publish("test/topic", 0, payload, false)
	require.NoError(t, err)

	// Wait for message
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "test/topic", msg.Topic)
	require.Equal(t, payload, msg.Payload)

	t.Log("Single node pub/sub test passed")
}
