// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package faults drives a real broker over TCP through a fault-injecting proxy
// and checks what clients observe when the network misbehaves: links that die
// silently, connections that reset mid-stream, clients that stop reading, and
// clients racing for the same session.
package faults

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startBroker runs a broker with the shipped defaults, adjusted by tune, and
// returns its TCP address.
func startBroker(t *testing.T, tune ...func(*config.Config)) string {
	t.Helper()
	return testutil.StartLocalBroker(t, tune...).TCPAddr
}

// connect opens an MQTT 3.1.1 session for clientID through addr.
func connect(t *testing.T, addr, clientID string, opts testutil.ConnectOptions) *testutil.TestMQTTClient {
	t.Helper()

	client := testutil.NewTestMQTTClient(t, &testutil.TestNode{TCPAddr: addr}, clientID)
	require.NoError(t, client.ConnectWithOptions(opts))
	t.Cleanup(client.DisconnectUngracefully)
	return client
}

// payloads returns the distinct payloads a client has received.
func payloads(client *testutil.TestMQTTClient) map[string]bool {
	seen := make(map[string]bool)
	for _, msg := range client.Messages().GetAll() {
		seen[string(msg.Payload)] = true
	}
	return seen
}

func publishNumbered(t *testing.T, publisher *testutil.TestMQTTClient, topic string, count int, qos byte) []string {
	t.Helper()

	sent := make([]string, count)
	for i := range count {
		sent[i] = fmt.Sprintf("%s-%03d", topic, i)
		require.NoError(t, publisher.Publish(topic, qos, []byte(sent[i]), false))
	}
	return sent
}

// A link that dies without a FIN or RST swallows everything the broker writes
// to it. None of it was acknowledged, so when the client resumes its session
// every QoS 1 message must be resent at once, flagged as a duplicate
// [MQTT-4.4.0-1] — well inside the broker's retry interval, which is for
// connections that stay up.
func TestQoS1RedeliveredAfterSilentLinkFailure(t *testing.T) {
	addr := startBroker(t)
	proxy := testutil.NewFaultProxy(t, addr)

	subscriber := connect(t, proxy.Addr(), "silent-sub", testutil.ConnectOptions{KeepAlive: 60})
	require.NoError(t, subscriber.Subscribe("silent/t", 1))

	proxy.Blackhole()
	publisher := connect(t, addr, "silent-pub", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
	sent := publishNumbered(t, publisher, "silent/t", 20, 1)

	proxy.Cut()
	proxy.Restore()

	returned := connect(t, proxy.Addr(), "silent-sub", testutil.ConnectOptions{KeepAlive: 60})
	require.Less(t, 5*time.Second, session.RetryTimeout, "the wait below must not cover a retry")
	require.Eventually(t, func() bool { return len(payloads(returned)) == len(sent) }, 5*time.Second, 20*time.Millisecond,
		"received %d of %d messages after reconnecting", len(payloads(returned)), len(sent))
	for _, msg := range returned.Messages().GetAll() {
		assert.True(t, msg.Dup, "redelivered %s without the DUP flag", msg.Payload)
	}
}

// A connection reset makes the broker see the client leave at once. QoS 1
// messages published while it is away are kept for its persistent session.
func TestQoS1QueuedAcrossConnectionReset(t *testing.T) {
	addr := startBroker(t)
	proxy := testutil.NewFaultProxy(t, addr)

	subscriber := connect(t, proxy.Addr(), "reset-sub", testutil.ConnectOptions{KeepAlive: 60})
	require.NoError(t, subscriber.Subscribe("reset/t", 1))
	proxy.Cut()

	publisher := connect(t, addr, "reset-pub", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
	sent := publishNumbered(t, publisher, "reset/t", 20, 1)

	returned := connect(t, proxy.Addr(), "reset-sub", testutil.ConnectOptions{KeepAlive: 60})
	require.Eventually(t, func() bool { return len(payloads(returned)) == len(sent) }, 10*time.Second, 50*time.Millisecond,
		"received %d of %d messages after reconnecting", len(payloads(returned)), len(sent))
}

// A client whose link goes dark is never heard from again. Keep-alive must
// end its session within one and a half intervals [MQTT-3.1.2-24] and publish
// its will.
func TestHalfOpenClientEndsByKeepAlive(t *testing.T) {
	addr := startBroker(t)
	proxy := testutil.NewFaultProxy(t, addr)

	watcher := connect(t, addr, "will-watcher", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
	require.NoError(t, watcher.Subscribe("wills/#", 1))

	connect(t, proxy.Addr(), "half-open", testutil.ConnectOptions{
		CleanSession: true,
		KeepAlive:    1,
		Will:         &testutil.WillMessage{Topic: "wills/half-open", Payload: []byte("gone"), QoS: 1},
	})
	start := time.Now()
	proxy.Blackhole()

	msg, err := watcher.WaitForMessage(5 * time.Second)
	require.NoError(t, err, "will was not published after the link went dark")
	assert.Equal(t, "gone", string(msg.Payload))
	assert.Less(t, time.Since(start), 4*time.Second, "keep-alive took too long to notice")

	// The session is gone, so the client can come back on a working link.
	proxy.Restore()
	connect(t, addr, "half-open", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
}

// A subscriber that stops reading must not hold up delivery to anyone else.
//
// This holds only with a per-connection send queue that disconnects a client
// once its queue is full. The shipped defaults write synchronously
// (max_send_queue_size: 0), so there the publisher's goroutine waits on the
// stalled socket for up to the listener's write timeout, and every other
// subscriber of the topic waits with it.
func TestStalledSubscriberDoesNotBlockOthers(t *testing.T) {
	addr := startBroker(t, func(cfg *config.Config) {
		cfg.Session.MaxSendQueueSize = 256
		cfg.Session.DisconnectOnFull = true
	})
	proxy := testutil.NewFaultProxy(t, addr)

	stalled := connect(t, proxy.Addr(), "stalled", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
	require.NoError(t, stalled.Subscribe("fanout/#", 0))
	healthy := connect(t, addr, "healthy", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
	require.NoError(t, healthy.Subscribe("fanout/#", 0))
	proxy.StallClient()

	// Enough to fill the stalled subscriber's socket buffers many times over.
	const count = 2000
	payload := make([]byte, 16<<10)
	publisher := connect(t, addr, "fanout-pub", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})

	start := time.Now()
	for range count {
		require.NoError(t, publisher.Publish("fanout/t", 0, payload, false))
	}
	require.Eventually(t, func() bool { return healthy.Messages().Count() == count }, 10*time.Second, 20*time.Millisecond,
		"healthy subscriber received %d of %d", healthy.Messages().Count(), count)
	assert.Less(t, time.Since(start), 10*time.Second, "the stalled subscriber held up the others")
}

// Clients racing to take over one session must leave exactly one live
// connection, and the session must keep working afterwards.
func TestConcurrentTakeoverLeavesOneWorkingSession(t *testing.T) {
	addr := startBroker(t)

	const (
		contenders = 8
		rounds     = 10
	)
	var wg sync.WaitGroup
	for round := range rounds {
		for contender := range contenders {
			wg.Go(func() {
				client := testutil.NewTestMQTTClient(t, &testutil.TestNode{TCPAddr: addr}, "contested")
				// A contender that loses the race is refused or taken over;
				// either is fine. Leave half gracefully and half abruptly.
				if err := client.ConnectWithOptions(testutil.ConnectOptions{KeepAlive: 60}); err != nil {
					return
				}
				if (round+contender)%2 == 0 {
					_ = client.Disconnect()
					return
				}
				client.DisconnectUngracefully()
			})
		}
		wg.Wait()
	}

	survivor := connect(t, addr, "contested", testutil.ConnectOptions{KeepAlive: 60})
	require.NoError(t, survivor.Subscribe("contested/t", 1))
	publisher := connect(t, addr, "contested-pub", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
	require.NoError(t, publisher.Publish("contested/t", 1, []byte("after-the-race"), false))

	require.Eventually(t, func() bool { return payloads(survivor)["after-the-race"] }, 5*time.Second, 20*time.Millisecond)
}
