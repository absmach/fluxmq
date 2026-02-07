// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCrossNode_QoS0_PublishSubscribe verifies QoS 0 message delivery across nodes.
func TestCrossNode_QoS0_PublishSubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Subscriber on node-0
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(t, node0, "qos0-sub")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	require.NoError(t, subscriber.Subscribe("sensor/temperature", 0))
	time.Sleep(500 * time.Millisecond) // Allow subscription propagation

	// Publisher on node-2 (different node)
	node2 := cluster.GetNodeByIndex(2)
	publisher := testutil.NewTestMQTTClient(t, node2, "qos0-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	// Publish message
	payload := []byte("22.5")
	require.NoError(t, publisher.Publish("sensor/temperature", 0, payload, false))

	// Verify delivery
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "sensor/temperature", msg.Topic)
	assert.Equal(t, payload, msg.Payload)
	assert.Equal(t, byte(0), msg.QoS)
}

// TestCrossNode_QoS1_PublishSubscribe verifies QoS 1 message delivery across nodes.
func TestCrossNode_QoS1_PublishSubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Subscriber on node-1
	node1 := cluster.GetNodeByIndex(1)
	subscriber := testutil.NewTestMQTTClient(t, node1, "qos1-sub")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	require.NoError(t, subscriber.Subscribe("sensor/humidity", 1))
	time.Sleep(500 * time.Millisecond)

	// Publisher on node-0
	node0 := cluster.GetNodeByIndex(0)
	publisher := testutil.NewTestMQTTClient(t, node0, "qos1-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	// Publish with QoS 1
	payload := []byte("65.3")
	require.NoError(t, publisher.Publish("sensor/humidity", 1, payload, false))

	// Verify delivery with QoS 1
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "sensor/humidity", msg.Topic)
	assert.Equal(t, payload, msg.Payload)
	assert.Equal(t, byte(1), msg.QoS)
}

// TestCrossNode_QoS2_PublishSubscribe verifies QoS 2 message delivery across nodes.
func TestCrossNode_QoS2_PublishSubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Subscriber on node-2
	node2 := cluster.GetNodeByIndex(2)
	subscriber := testutil.NewTestMQTTClient(t, node2, "qos2-sub")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	require.NoError(t, subscriber.Subscribe("alerts/critical", 2))
	time.Sleep(500 * time.Millisecond)

	// Publisher on node-1
	node1 := cluster.GetNodeByIndex(1)
	publisher := testutil.NewTestMQTTClient(t, node1, "qos2-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	// Publish with QoS 2
	payload := []byte("fire detected")
	require.NoError(t, publisher.Publish("alerts/critical", 2, payload, false))

	// Verify delivery with QoS 2
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "alerts/critical", msg.Topic)
	assert.Equal(t, payload, msg.Payload)
	assert.Equal(t, byte(2), msg.QoS)
}

// TestCrossNode_StreamReplayFromFirstOffsetAfterLateConsumer mirrors the reported cluster stream issue:
// publish stream messages first, then attach a stream consumer at earliest offset and verify replay.
func TestCrossNode_StreamReplayFromFirstOffsetAfterLateConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	pubNode := cluster.GetNodeByIndex(0)
	subNode := cluster.GetNodeByIndex(2)
	require.NotNil(t, pubNode)
	require.NotNil(t, subNode)

	publisher := testutil.NewTestMQTTClient(t, pubNode, "stream-pub-late")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	consumer := testutil.NewTestMQTTClient(t, subNode, "stream-sub-late")
	require.NoError(t, consumer.Connect(true))
	defer consumer.Disconnect()

	pubQM := pubNode.Broker.GetQueueManager()
	subQM := subNode.Broker.GetQueueManager()
	require.NotNil(t, pubQM)
	require.NotNil(t, subQM)

	queueName := fmt.Sprintf("demo-events-%d", time.Now().UnixNano())
	queueTopic := "$queue/" + queueName

	ctx := context.Background()
	queueCfg := qtypes.DefaultQueueConfig(queueName, queueTopic+"/#")
	queueCfg.Type = qtypes.QueueTypeStream
	require.NoError(t, pubQM.CreateQueue(ctx, queueCfg))

	const messageCount = 5
	expectedPayloads := make([][]byte, 0, messageCount)
	for i := 1; i <= messageCount; i++ {
		payload := []byte(fmt.Sprintf(`{"event":"user.action","seq":%d}`, i))
		expectedPayloads = append(expectedPayloads, payload)
		require.NoError(t, publisher.Publish(queueTopic, 1, payload, false))
	}

	// Ensure publishes land before stream consumer registration.
	time.Sleep(300 * time.Millisecond)

	cursor := &qtypes.CursorOption{
		Position: qtypes.CursorEarliest,
		Mode:     qtypes.GroupModeStream,
	}
	require.NoError(t, subQM.SubscribeWithCursor(ctx, queueName, "#", consumer.ClientID, "demo-readers", "", cursor))

	msgs, err := consumer.WaitForMessages(messageCount, 12*time.Second)
	require.NoError(t, err)
	require.Len(t, msgs, messageCount)

	for i, msg := range msgs {
		assert.Equal(t, queueTopic, msg.Topic)
		assert.Equal(t, expectedPayloads[i], msg.Payload)
	}

	deliveries := subNode.QueueDeliveries()
	offsets := make([]string, 0, messageCount)
	for _, d := range deliveries {
		if d.ClientID != consumer.ClientID || d.Message == nil {
			continue
		}
		if d.Message.Properties["queue"] != queueName {
			continue
		}
		offset := d.Message.Properties["x-stream-offset"]
		if offset != "" {
			offsets = append(offsets, offset)
		}
	}
	require.GreaterOrEqual(t, len(offsets), messageCount)
	assert.Equal(t, []string{"0", "1", "2", "3", "4"}, offsets[:messageCount])
}

// TestCrossNode_MultipleSubscribers verifies message delivery to multiple subscribers on different nodes.
func TestCrossNode_MultipleSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Create subscribers on different nodes
	node0 := cluster.GetNodeByIndex(0)
	sub1 := testutil.NewTestMQTTClient(t, node0, "multi-sub-1")
	require.NoError(t, sub1.Connect(true))
	defer sub1.Disconnect()

	node1 := cluster.GetNodeByIndex(1)
	sub2 := testutil.NewTestMQTTClient(t, node1, "multi-sub-2")
	require.NoError(t, sub2.Connect(true))
	defer sub2.Disconnect()

	node2 := cluster.GetNodeByIndex(2)
	sub3 := testutil.NewTestMQTTClient(t, node2, "multi-sub-3")
	require.NoError(t, sub3.Connect(true))
	defer sub3.Disconnect()

	// All subscribe to same topic
	topic := "broadcast/message"
	require.NoError(t, sub1.Subscribe(topic, 0))
	require.NoError(t, sub2.Subscribe(topic, 0))
	require.NoError(t, sub3.Subscribe(topic, 0))
	time.Sleep(2 * time.Second) // Longer wait for subscription propagation

	// Publisher on node-0
	publisher := testutil.NewTestMQTTClient(t, node0, "multi-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	time.Sleep(500 * time.Millisecond) // Let publisher connection settle

	payload := []byte("broadcast to all")
	require.NoError(t, publisher.Publish(topic, 0, payload, false))

	// All subscribers should receive the message
	msg1, err := sub1.WaitForMessage(10 * time.Second)
	require.NoError(t, err, "subscriber 1 did not receive message")
	assert.Equal(t, topic, msg1.Topic)
	assert.Equal(t, payload, msg1.Payload)

	msg2, err := sub2.WaitForMessage(10 * time.Second)
	require.NoError(t, err, "subscriber 2 did not receive message")
	assert.Equal(t, topic, msg2.Topic)
	assert.Equal(t, payload, msg2.Payload)

	msg3, err := sub3.WaitForMessage(10 * time.Second)
	require.NoError(t, err, "subscriber 3 did not receive message")
	assert.Equal(t, topic, msg3.Topic)
	assert.Equal(t, payload, msg3.Payload)
}

// TestCrossNode_WildcardSubscriptions verifies wildcard subscriptions work across nodes.
func TestCrossNode_WildcardSubscriptions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	tests := []struct {
		name         string
		subscription string
		pubTopics    []string
		expected     int // number of messages expected
	}{
		{
			name:         "single level wildcard",
			subscription: "sensor/+/temperature",
			pubTopics:    []string{"sensor/room1/temperature", "sensor/room2/temperature", "sensor/room1/humidity"},
			expected:     2,
		},
		{
			name:         "multi level wildcard",
			subscription: "sensor/#",
			pubTopics:    []string{"sensor/room1/temperature", "sensor/room2/humidity", "alerts/critical"},
			expected:     2,
		},
		{
			name:         "mixed wildcards",
			subscription: "device/+/sensor/#",
			pubTopics:    []string{"device/123/sensor/temp", "device/123/sensor/temp/high", "device/123/status"},
			expected:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Subscriber on node-1
			node1 := cluster.GetNodeByIndex(1)
			subscriber := testutil.NewTestMQTTClient(t, node1, "wildcard-sub-"+tt.name)
			require.NoError(t, subscriber.Connect(true))
			defer subscriber.Disconnect()

			require.NoError(t, subscriber.Subscribe(tt.subscription, 0))
			time.Sleep(500 * time.Millisecond)

			// Publisher on node-2
			node2 := cluster.GetNodeByIndex(2)
			publisher := testutil.NewTestMQTTClient(t, node2, "wildcard-pub-"+tt.name)
			require.NoError(t, publisher.Connect(true))
			defer publisher.Disconnect()

			// Publish to all topics
			for _, topic := range tt.pubTopics {
				require.NoError(t, publisher.Publish(topic, 0, []byte("data"), false))
			}

			// Count received messages
			received := 0
			for received < tt.expected {
				_, err := subscriber.WaitForMessage(5 * time.Second)
				if err != nil {
					t.Fatalf("timeout: expected %d messages, got %d (error: %v)", tt.expected, received, err)
				}
				received++
			}

			// Ensure no extra messages arrive
			_, err := subscriber.WaitForMessage(500 * time.Millisecond)
			if err == nil {
				t.Fatalf("unexpected extra message received")
			}

			assert.Equal(t, tt.expected, received)
		})
	}
}

// TestCrossNode_SubscriptionPropagation verifies subscriptions propagate across all nodes.
// NOTE: Test times out when publishing from node-1/node-2 to subscriber on node-0.
// Works fine when publisher and subscriber are on same node. May be related to routing or timing.
func TestCrossNode_SubscriptionPropagation(t *testing.T) {
	t.Skip("Subscription propagation test times out - needs routing investigation")

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Subscribe on node-0
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(t, node0, "prop-sub")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	topic := "propagation/test"
	require.NoError(t, subscriber.Subscribe(topic, 0))
	time.Sleep(1 * time.Second) // Allow propagation

	// Publish from each node, subscriber should receive all
	nodes := []*testutil.TestNode{
		cluster.GetNodeByIndex(0),
		cluster.GetNodeByIndex(1),
		cluster.GetNodeByIndex(2),
	}

	for i, node := range nodes {
		publisher := testutil.NewTestMQTTClient(t, node, "prop-pub-"+string(rune('0'+i)))
		require.NoError(t, publisher.Connect(true))

		payload := []byte("message from node " + string(rune('0'+i)))
		require.NoError(t, publisher.Publish(topic, 0, payload, false))

		msg, err := subscriber.WaitForMessage(5 * time.Second)
		require.NoError(t, err)
		assert.Equal(t, topic, msg.Topic)
		assert.Equal(t, payload, msg.Payload)

		publisher.Disconnect()
	}
}

// TestCrossNode_UnsubscribePropagation verifies unsubscribe propagates and stops delivery.
// NOTE: Requires Unsubscribe() method in testutil.TestMQTTClient (see plan.md Task #7).
func TestCrossNode_UnsubscribePropagation(t *testing.T) {
	t.Skip("Requires Unsubscribe() implementation in test client (plan.md Task #7)")

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Subscribe on node-0
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(t, node0, "unsub-test")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	topic := "unsub/test"
	require.NoError(t, subscriber.Subscribe(topic, 0))
	time.Sleep(500 * time.Millisecond)

	// Publisher on node-1
	node1 := cluster.GetNodeByIndex(1)
	publisher := testutil.NewTestMQTTClient(t, node1, "unsub-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	// Publish first message - should be received
	require.NoError(t, publisher.Publish(topic, 0, []byte("before unsub"), false))
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, []byte("before unsub"), msg.Payload)

	// After implementing Unsubscribe() in testutil client, test that:
	// 1. Unsubscribe propagates across cluster
	// 2. Message delivery stops after unsubscribe
}

// TestCrossNode_RetainedMessages verifies retained messages work across nodes.
// NOTE: Retained messages currently only work within single node (local storage).
// Cross-node delivery requires centralized storage (plan.md Task #8).
func TestCrossNode_RetainedMessages(t *testing.T) {
	t.Skip("Retained message cross-node delivery requires centralized storage (plan.md Task #8)")

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Publisher on node-0 publishes retained message
	node0 := cluster.GetNodeByIndex(0)
	publisher := testutil.NewTestMQTTClient(t, node0, "retained-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	topic := "status/server"
	payload := []byte("online")
	require.NoError(t, publisher.Publish(topic, 0, payload, true)) // retain=true
	time.Sleep(500 * time.Millisecond)

	// Subscriber on node-2 (different node) should receive retained message immediately
	node2 := cluster.GetNodeByIndex(2)
	subscriber := testutil.NewTestMQTTClient(t, node2, "retained-sub")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	require.NoError(t, subscriber.Subscribe(topic, 0))

	// Should receive retained message
	msg, err := subscriber.WaitForMessage(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, topic, msg.Topic)
	assert.Equal(t, payload, msg.Payload)
	assert.True(t, msg.Retain, "expected retain flag to be set")
}

// TestCrossNode_HighThroughput verifies cluster can handle high message throughput.
// NOTE: Performance test - can be slow, enable manually when needed.
func TestCrossNode_HighThroughput(t *testing.T) {
	t.Skip("Performance test - enable manually when needed")

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := testutil.NewTestCluster(t, 3)
	defer cluster.Stop()

	require.NoError(t, cluster.Start())
	require.NoError(t, cluster.WaitForClusterReady(30*time.Second))

	// Subscriber on node-2
	node2 := cluster.GetNodeByIndex(2)
	subscriber := testutil.NewTestMQTTClient(t, node2, "throughput-sub")
	require.NoError(t, subscriber.Connect(true))
	defer subscriber.Disconnect()

	topic := "throughput/test"
	require.NoError(t, subscriber.Subscribe(topic, 0))
	time.Sleep(500 * time.Millisecond)

	// Publisher on node-0
	node0 := cluster.GetNodeByIndex(0)
	publisher := testutil.NewTestMQTTClient(t, node0, "throughput-pub")
	require.NoError(t, publisher.Connect(true))
	defer publisher.Disconnect()

	// Publish 100 messages rapidly
	messageCount := 100
	payload := []byte("high throughput test message")

	start := time.Now()
	for i := 0; i < messageCount; i++ {
		require.NoError(t, publisher.Publish(topic, 0, payload, false))
	}

	// Receive all messages
	received := 0
	for received < messageCount {
		_, err := subscriber.WaitForMessage(10 * time.Second)
		require.NoError(t, err)
		received++
	}

	elapsed := time.Since(start)
	t.Logf("Delivered %d messages in %v (%.2f msg/sec)", messageCount, elapsed, float64(messageCount)/elapsed.Seconds())

	assert.Equal(t, messageCount, received)
	assert.Less(t, elapsed, 10*time.Second, "throughput too low")
}

// BenchmarkCrossNode_MessageLatency measures cross-node message delivery latency.
// NOTE: Benchmarks are disabled for now - enable when needed for performance testing.
func BenchmarkCrossNode_MessageLatency(b *testing.B) {
	b.Skip("Benchmarks disabled - enable manually when needed")

	cluster := testutil.NewTestCluster(&testing.T{}, 3)
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		b.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForClusterReady(30 * time.Second); err != nil {
		b.Fatalf("cluster not ready: %v", err)
	}

	// Setup subscriber on node-1
	node1 := cluster.GetNodeByIndex(1)
	subscriber := testutil.NewTestMQTTClient(&testing.T{}, node1, "bench-sub")
	if err := subscriber.Connect(true); err != nil {
		b.Fatalf("failed to connect subscriber: %v", err)
	}
	defer subscriber.Disconnect()

	if err := subscriber.Subscribe("bench/latency", 0); err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Setup publisher on node-2
	node2 := cluster.GetNodeByIndex(2)
	publisher := testutil.NewTestMQTTClient(&testing.T{}, node2, "bench-pub")
	if err := publisher.Connect(true); err != nil {
		b.Fatalf("failed to connect publisher: %v", err)
	}
	defer publisher.Disconnect()

	payload := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()

		if err := publisher.Publish("bench/latency", 0, payload, false); err != nil {
			b.Fatalf("publish failed: %v", err)
		}

		if _, err := subscriber.WaitForMessage(5 * time.Second); err != nil {
			b.Fatalf("receive failed: %v", err)
		}

		latency := time.Since(start)
		b.ReportMetric(float64(latency.Microseconds()), "µs/op")
	}
}

// BenchmarkCrossNode_Throughput measures maximum message throughput across nodes.
// NOTE: Benchmarks are disabled for now - enable when needed for performance testing.
func BenchmarkCrossNode_Throughput(b *testing.B) {
	b.Skip("Benchmarks disabled - enable manually when needed")

	cluster := testutil.NewTestCluster(&testing.T{}, 3)
	defer cluster.Stop()

	if err := cluster.Start(); err != nil {
		b.Fatalf("failed to start cluster: %v", err)
	}

	if err := cluster.WaitForClusterReady(30 * time.Second); err != nil {
		b.Fatalf("cluster not ready: %v", err)
	}

	// Setup subscriber on node-0
	node0 := cluster.GetNodeByIndex(0)
	subscriber := testutil.NewTestMQTTClient(&testing.T{}, node0, "bench-throughput-sub")
	if err := subscriber.Connect(true); err != nil {
		b.Fatalf("failed to connect subscriber: %v", err)
	}
	defer subscriber.Disconnect()

	if err := subscriber.Subscribe("bench/throughput", 0); err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Setup publisher on node-2
	node2 := cluster.GetNodeByIndex(2)
	publisher := testutil.NewTestMQTTClient(&testing.T{}, node2, "bench-throughput-pub")
	if err := publisher.Connect(true); err != nil {
		b.Fatalf("failed to connect publisher: %v", err)
	}
	defer publisher.Disconnect()

	payload := []byte("throughput benchmark message with some payload data")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := publisher.Publish("bench/throughput", 0, payload, false); err != nil {
				b.Fatalf("publish failed: %v", err)
			}
		}
	})
}
