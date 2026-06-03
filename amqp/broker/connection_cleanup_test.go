// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

// testCluster is a stub cluster.Cluster for testing cleanup timeout behavior.
// Set blockMethods to true to make cluster calls block until the context expires.
type testCluster struct {
	mu           sync.Mutex
	deadlines    map[string]time.Time
	blockMethods bool
}

func newTestCluster(block bool) *testCluster {
	return &testCluster{deadlines: make(map[string]time.Time), blockMethods: block}
}

func (c *testCluster) record(method string, ctx context.Context) {
	c.mu.Lock()
	d, _ := ctx.Deadline()
	c.deadlines[method] = d
	c.mu.Unlock()
}

func (c *testCluster) block(ctx context.Context) error {
	if c.blockMethods {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *testCluster) hasDeadline(method string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.deadlines[method]
	return !d.IsZero()
}

func (c *testCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	c.record("AcquireSession", ctx)
	return c.block(ctx)
}

func (c *testCluster) ReleaseSession(ctx context.Context, clientID string) error {
	c.record("ReleaseSession", ctx)
	return c.block(ctx)
}

func (c *testCluster) RemoveAllSubscriptions(ctx context.Context, clientID string) error {
	c.record("RemoveAllSubscriptions", ctx)
	return c.block(ctx)
}

func (c *testCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error {
	c.record("RemoveSubscription", ctx)
	return c.block(ctx)
}

// Remaining cluster.Cluster methods — all no-ops for cleanup tests.
func (c *testCluster) GetSessionOwner(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}

func (c *testCluster) WatchSessionOwner(_ context.Context, _ string) <-chan cluster.OwnershipChange {
	return nil
}

func (c *testCluster) AddSubscription(_ context.Context, _, _ string, _ byte, _ storage.SubscribeOptions) error {
	return nil
}

func (c *testCluster) GetSubscriptionsForClient(_ context.Context, _ string) ([]*storage.Subscription, error) {
	return nil, nil
}

func (c *testCluster) GetSubscribersForTopic(_ context.Context, _ string) ([]*storage.Subscription, error) {
	return nil, nil
}

func (c *testCluster) RegisterQueueConsumer(_ context.Context, _ *cluster.QueueConsumerInfo) error {
	return nil
}
func (c *testCluster) UnregisterQueueConsumer(_ context.Context, _, _, _ string) error { return nil }
func (c *testCluster) ListQueueConsumers(_ context.Context, _ string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *testCluster) ListQueueConsumersByGroup(_ context.Context, _, _ string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *testCluster) ListAllQueueConsumers(_ context.Context) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *testCluster) ForwardQueuePublish(_ context.Context, _, _ string, _ []byte, _ map[string]string, _ bool) error {
	return nil
}
func (c *testCluster) ForwardGroupOp(_ context.Context, _, _ string, _ []byte) error { return nil }
func (c *testCluster) IsLeader(_ context.Context) bool                               { return true }
func (c *testCluster) WaitForLeader(_ context.Context) error                         { return nil }
func (c *testCluster) Start() error                                                  { return nil }
func (c *testCluster) Stop() error                                                   { return nil }
func (c *testCluster) NodeID() string                                                { return "test" }
func (c *testCluster) Nodes() []cluster.NodeInfo                                     { return nil }
func (c *testCluster) Retained() storage.RetainedStore                               { return nil }
func (c *testCluster) Wills() storage.WillStore                                      { return nil }
func (c *testCluster) RoutePublish(_ context.Context, _ string, _ []byte, _ byte, _ bool, _ map[string]string) error {
	return nil
}

func (c *testCluster) TakeoverSession(_ context.Context, _, _, _ string) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (c *testCluster) RouteQueueMessage(_ context.Context, _, _, _ string, _ *cluster.QueueMessage) error {
	return nil
}

func newTestConn(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func newTestConnection(t *testing.T, b *Broker, serverConn net.Conn) *Connection {
	t.Helper()
	return &Connection{
		broker:   b,
		conn:     serverConn,
		writer:   bufio.NewWriter(io.Discard),
		closeCh:  make(chan struct{}),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		connID:   "test-conn",
		channels: make(map[uint16]*Channel),
	}
}

func TestConnectionCleanupClusterCallsHaveDeadline(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	cl := newTestCluster(false)
	b.SetCluster(cl)

	_, server := newTestConn(t)
	conn := newTestConnection(t, b, server)

	conn.cleanup()

	require.True(t, cl.hasDeadline("RemoveAllSubscriptions"), "RemoveAllSubscriptions should receive a context with deadline")
	require.True(t, cl.hasDeadline("ReleaseSession"), "ReleaseSession should receive a context with deadline")
}

func TestChannelCleanupClusterCallsHaveDeadline(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	cl := newTestCluster(false)
	b.SetCluster(cl)

	_, server := newTestConn(t)
	conn := newTestConnection(t, b, server)
	ch := newChannel(conn, 1)
	ch.consumers[testCtag] = &consumer{
		tag:        testCtag,
		queue:      "test/topic",
		mqttFilter: "test/+",
	}

	ch.cleanup()

	require.True(t, cl.hasDeadline("RemoveSubscription"), "RemoveSubscription should receive a context with deadline")
}

func TestConnectionCleanupCompletesWhenClusterStalls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stall test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	cl := newTestCluster(true) // block until context expires
	b.SetCluster(cl)

	_, server := newTestConn(t)
	conn := newTestConnection(t, b, server)

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.cleanup()
	}()

	select {
	case <-done:
	case <-time.After(clusterOpTimeout + time.Second):
		t.Fatal("cleanup() blocked beyond clusterOpTimeout")
	}
}

func TestChannelCleanupCompletesWhenClusterStalls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stall test in short mode")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	cl := newTestCluster(true) // block until context expires
	b.SetCluster(cl)

	_, server := newTestConn(t)
	conn := newTestConnection(t, b, server)
	ch := newChannel(conn, 1)
	ch.consumers[testCtag] = &consumer{
		tag:        testCtag,
		queue:      "test/topic",
		mqttFilter: "test/+",
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.cleanup()
	}()

	select {
	case <-done:
	case <-time.After(clusterOpTimeout + time.Second):
		t.Fatal("channel cleanup() blocked beyond clusterOpTimeout")
	}
}
