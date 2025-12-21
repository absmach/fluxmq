// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
	"time"

	"github.com/absmach/mqtt/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionTakeover_BasicReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	err = cluster.WaitForClusterReady(60 * time.Second)
	require.NoError(t, err)

	_, err = cluster.WaitForLeader(30 * time.Second)
	require.NoError(t, err)

	// Connect client to node-0 with clean_session=false (persistent session)
	node0 := cluster.GetNodeByIndex(0)
	client := testutil.NewTestMQTTClient(t, node0, "session-client-1")
	err = client.Connect(false) // clean_session=false
	require.NoError(t, err)

	// Subscribe to topic
	err = client.Subscribe("takeover/test", 0)
	require.NoError(t, err)

	// Wait for subscription to propagate
	time.Sleep(2 * time.Second)

	// Disconnect ungracefully (close connection without DISCONNECT packet)
	if client.Conn != nil {
		client.Conn.Close()
		client.Conn = nil
	}

	t.Log("Client disconnected from node-0")

	// Wait a bit for session to be stored
	time.Sleep(1 * time.Second)

	// Reconnect to different node (node-1)
	node1 := cluster.GetNodeByIndex(1)
	err = client.Reconnect(node1)
	require.NoError(t, err)
	time.Sleep(2 * time.Second) // Wait for takeover to settle

	t.Log("Client reconnected to node-1")

	// Verify session was taken over by publishing from another client
	publisher := testutil.NewTestMQTTClient(t, node1, "publisher-takeover")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect()

	// Publish message
	payload := []byte("takeover message")
	err = publisher.Publish("takeover/test", 0, payload, false)
	require.NoError(t, err)

	// Client should receive message (subscription preserved)
	msg, err := client.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "takeover/test", msg.Topic)
	assert.Equal(t, payload, msg.Payload)

	client.Disconnect()

	t.Log("Session takeover successful")
}

func TestSessionTakeover_QoS1Messages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	err = cluster.WaitForClusterReady(60 * time.Second)
	require.NoError(t, err)

	_, err = cluster.WaitForLeader(30 * time.Second)
	require.NoError(t, err)

	// Connect subscriber to node-0 with persistent session
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(t, node0, "qos1-subscriber")
	err = subscriber.Connect(false) // clean_session=false
	require.NoError(t, err)

	// Subscribe with QoS 1
	err = subscriber.Subscribe("qos1/test", 1)
	require.NoError(t, err)

	// Wait for subscription to propagate
	time.Sleep(2 * time.Second)

	// Disconnect ungracefully
	if subscriber.Conn != nil {
		subscriber.Conn.Close()
		subscriber.Conn = nil
	}

	t.Log("Subscriber disconnected from node-0")

	// Publish message while client is offline (QoS 1)
	node2 := cluster.GetNodeByIndex(2)
	publisher := testutil.NewTestMQTTClient(t, node2, "qos1-publisher")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect()

	payload := []byte("offline qos1 message")
	err = publisher.Publish("qos1/test", 1, payload, false)
	require.NoError(t, err)

	t.Log("Published QoS 1 message while subscriber offline")

	// Wait for message to be persisted
	time.Sleep(1 * time.Second)

	// Reconnect subscriber to different node (node-1)
	node1 := cluster.GetNodeByIndex(1)
	err = subscriber.Reconnect(node1)
	require.NoError(t, err)
	time.Sleep(2 * time.Second) // Wait for takeover to settle

	t.Log("Subscriber reconnected to node-1")

	// Should receive the message published while offline
	msg, err := subscriber.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "qos1/test", msg.Topic)
	assert.Equal(t, payload, msg.Payload)

	subscriber.Disconnect()

	t.Log("QoS 1 offline message delivered successfully")
}

func TestSessionTakeover_MultipleClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	err := cluster.Start()
	require.NoError(t, err)

	err = cluster.WaitForClusterReady(60 * time.Second)
	require.NoError(t, err)

	_, err = cluster.WaitForLeader(30 * time.Second)
	require.NoError(t, err)

	node0 := cluster.GetNodeByIndex(0)
	node1 := cluster.GetNodeByIndex(1)
	node2 := cluster.GetNodeByIndex(2)

	// Connect multiple clients to different nodes
	client1 := testutil.NewTestMQTTClient(t, node0, "multi-client-1")
	err = client1.Connect(false)
	require.NoError(t, err)
	err = client1.Subscribe("multi/test", 0)
	require.NoError(t, err)

	client2 := testutil.NewTestMQTTClient(t, node1, "multi-client-2")
	err = client2.Connect(false)
	require.NoError(t, err)
	err = client2.Subscribe("multi/test", 0)
	require.NoError(t, err)

	client3 := testutil.NewTestMQTTClient(t, node2, "multi-client-3")
	err = client3.Connect(false)
	require.NoError(t, err)
	err = client3.Subscribe("multi/test", 0)
	require.NoError(t, err)

	// Wait for subscriptions to propagate
	time.Sleep(2 * time.Second)

	// Disconnect all clients ungracefully
	if client1.Conn != nil {
		client1.Conn.Close()
		client1.Conn = nil
	}
	if client2.Conn != nil {
		client2.Conn.Close()
		client2.Conn = nil
	}
	if client3.Conn != nil {
		client3.Conn.Close()
		client3.Conn = nil
	}

	t.Log("All clients disconnected")

	time.Sleep(1 * time.Second)

	// Reconnect all clients to different nodes (round-robin)
	err = client1.Reconnect(node1) // was on node-0, now on node-1
	require.NoError(t, err)
	err = client2.Reconnect(node2) // was on node-1, now on node-2
	require.NoError(t, err)
	err = client3.Reconnect(node0) // was on node-2, now on node-0
	require.NoError(t, err)
	time.Sleep(2 * time.Second) // Wait for takeovers to settle

	t.Log("All clients reconnected to different nodes")

	// Publish message
	publisher := testutil.NewTestMQTTClient(t, node0, "multi-publisher")
	err = publisher.Connect(true)
	require.NoError(t, err)
	defer publisher.Disconnect()

	payload := []byte("multi-client broadcast")
	err = publisher.Publish("multi/test", 0, payload, false)
	require.NoError(t, err)

	// All three clients should receive the message
	msg1, err := client1.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "multi/test", msg1.Topic)
	assert.Equal(t, payload, msg1.Payload)

	msg2, err := client2.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "multi/test", msg2.Topic)
	assert.Equal(t, payload, msg2.Payload)

	msg3, err := client3.WaitForMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "multi/test", msg3.Topic)
	assert.Equal(t, payload, msg3.Payload)

	client1.Disconnect()
	client2.Disconnect()
	client3.Disconnect()

	t.Log("Multiple client takeover successful")
}
