// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	logStorage "github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/server/tcp"
	brokerstorage "github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/badger"
	"github.com/stretchr/testify/require"
)

// TestCluster represents a cluster of test nodes.
type TestCluster struct {
	t       *testing.T
	Nodes   []*TestNode
	mu      sync.RWMutex
	stopped bool
}

func allocateUniquePort(t *testing.T, used map[int]struct{}) int {
	t.Helper()

	for {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		if _, exists := used[port]; exists {
			continue
		}
		used[port] = struct{}{}
		return port
	}
}

// TestNode represents a single node in the test cluster.
type TestNode struct {
	ID         string
	Broker     *broker.Broker
	Cluster    *cluster.EtcdCluster
	TCPServer  *tcp.Server
	Port       int
	GRPCPort   int
	EtcdPort   int
	PeerPort   int
	DataDir    string
	TCPAddr    string
	GRPCAddr   string
	EtcdAddr   string
	PeerAddr   string
	ctx        context.Context
	cancel     context.CancelFunc
	tcpStopped chan struct{}
	queueStore *logStorage.Adapter

	queueDeliveriesMu sync.RWMutex
	queueDeliveries   []QueueDelivery
}

// QueueDelivery captures a queue delivery observed by the test node queue manager.
type QueueDelivery struct {
	ClientID string
	Message  *brokerstorage.Message
}

func cloneDeliveryMessage(msg *brokerstorage.Message) *brokerstorage.Message {
	if msg == nil {
		return nil
	}

	clone := &brokerstorage.Message{
		Topic:         msg.Topic,
		ContentType:   msg.ContentType,
		ResponseTopic: msg.ResponseTopic,
		QoS:           msg.QoS,
		Retain:        msg.Retain,
	}
	if len(msg.Properties) > 0 {
		clone.Properties = make(map[string]string, len(msg.Properties))
		for k, v := range msg.Properties {
			clone.Properties[k] = v
		}
	}
	if payload := msg.GetPayload(); len(payload) > 0 {
		clone.Payload = make([]byte, len(payload))
		copy(clone.Payload, payload)
	}

	return clone
}

func (n *TestNode) recordQueueDelivery(clientID string, msg any) {
	deliveryMsg, ok := msg.(*brokerstorage.Message)
	if !ok || deliveryMsg == nil {
		return
	}

	n.queueDeliveriesMu.Lock()
	n.queueDeliveries = append(n.queueDeliveries, QueueDelivery{
		ClientID: clientID,
		Message:  cloneDeliveryMessage(deliveryMsg),
	})
	n.queueDeliveriesMu.Unlock()
}

// QueueDeliveries returns a snapshot of captured queue deliveries.
func (n *TestNode) QueueDeliveries() []QueueDelivery {
	n.queueDeliveriesMu.RLock()
	defer n.queueDeliveriesMu.RUnlock()

	out := make([]QueueDelivery, len(n.queueDeliveries))
	for i, d := range n.queueDeliveries {
		out[i] = QueueDelivery{
			ClientID: d.ClientID,
			Message:  cloneDeliveryMessage(d.Message),
		}
	}
	return out
}

// NewTestCluster creates a new test cluster with the specified number of nodes.
// Nodes are numbered starting from 0.
func NewTestCluster(t *testing.T, nodeCount int) *TestCluster {
	require.True(t, nodeCount > 0, "nodeCount must be positive")
	require.True(t, nodeCount <= 10, "nodeCount must be <= 10 for port allocation")

	tc := &TestCluster{
		t:     t,
		Nodes: make([]*TestNode, nodeCount),
	}

	// Allocate ports dynamically to avoid collisions across concurrent/stale test runs.
	usedPorts := make(map[int]struct{})
	tcpPorts := make([]int, nodeCount)
	grpcPorts := make([]int, nodeCount)
	etcdPorts := make([]int, nodeCount)
	peerPorts := make([]int, nodeCount)
	for i := 0; i < nodeCount; i++ {
		tcpPorts[i] = allocateUniquePort(t, usedPorts)
		grpcPorts[i] = allocateUniquePort(t, usedPorts)
		etcdPorts[i] = allocateUniquePort(t, usedPorts)
		peerPorts[i] = allocateUniquePort(t, usedPorts)
	}

	// Build initial cluster string for etcd
	initialCluster := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		peerAddr := fmt.Sprintf("127.0.0.1:%d", peerPorts[i])
		initialCluster[i] = fmt.Sprintf("%s=http://%s", nodeID, peerAddr)
	}
	initialClusterStr := ""
	for i, entry := range initialCluster {
		if i > 0 {
			initialClusterStr += ","
		}
		initialClusterStr += entry
	}

	// Create nodes
	for i := 0; i < nodeCount; i++ {
		node := tc.createNode(i, tcpPorts[i], grpcPorts[i], etcdPorts[i], peerPorts[i], initialClusterStr)
		tc.Nodes[i] = node
	}

	return tc
}

func (tc *TestCluster) createNode(index, tcpPort, grpcPort, etcdPort, peerPort int, initialCluster string) *TestNode {
	nodeID := fmt.Sprintf("node-%d", index)

	// Create temp directory for node data
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("mqtt-test-node-%d-*", index))
	require.NoError(tc.t, err)

	tcpAddr := fmt.Sprintf("127.0.0.1:%d", tcpPort)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	etcdAddr := fmt.Sprintf("127.0.0.1:%d", etcdPort)
	peerAddr := fmt.Sprintf("127.0.0.1:%d", peerPort)

	ctx, cancel := context.WithCancel(context.Background())

	return &TestNode{
		ID:         nodeID,
		Port:       tcpPort,
		GRPCPort:   grpcPort,
		EtcdPort:   etcdPort,
		PeerPort:   peerPort,
		DataDir:    tmpDir,
		TCPAddr:    tcpAddr,
		GRPCAddr:   grpcAddr,
		EtcdAddr:   etcdAddr,
		PeerAddr:   peerAddr,
		ctx:        ctx,
		cancel:     cancel,
		tcpStopped: make(chan struct{}),
	}
}

// Start starts all nodes in the cluster.
func (tc *TestCluster) Start() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Build peer transport map
	peerTransports := make(map[string]string)
	for _, node := range tc.Nodes {
		peerTransports[node.ID] = node.GRPCAddr
	}

	// Start all nodes quickly in sequence
	// For a new cluster, ALL nodes need Bootstrap=true (ClusterState="new")
	// Nodes must start within ~60s to form quorum before first node times out
	for _, node := range tc.Nodes {
		go func(n *TestNode) {
			if err := tc.startNode(n, true, peerTransports); err != nil {
				tc.t.Logf("Failed to start %s: %v", n.ID, err)
			}
		}(node)
		// Tiny delay to avoid port binding races
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for cluster to stabilize
	time.Sleep(10 * time.Second)

	return nil
}

func (tc *TestCluster) startNode(node *TestNode, bootstrap bool, peerTransports map[string]string) error {
	storageDir := fmt.Sprintf("%s/badger", node.DataDir)
	etcdDir := fmt.Sprintf("%s/etcd", node.DataDir)

	// Build initial cluster string
	initialCluster := ""
	for i, n := range tc.Nodes {
		if i > 0 {
			initialCluster += ","
		}
		initialCluster += fmt.Sprintf("%s=http://%s", n.ID, n.PeerAddr)
	}

	tc.t.Logf("Node %s initialCluster: %s", node.ID, initialCluster)
	tc.t.Logf("Node %s will bind to peer: %s, client: %s", node.ID, node.PeerAddr, node.EtcdAddr)

	// Create BadgerDB storage
	store, err := badger.New(badger.Config{Dir: storageDir})
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	// Create cluster
	// Create null logger to reduce test noise
	nullLogger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))

	clusterCfg := &cluster.EtcdConfig{
		NodeID:         node.ID,
		DataDir:        etcdDir,
		BindAddr:       node.PeerAddr,
		ClientAddr:     node.EtcdAddr,
		AdvertiseAddr:  node.PeerAddr,
		InitialCluster: initialCluster,
		TransportAddr:  node.GRPCAddr,
		PeerTransports: peerTransports,
		Bootstrap:      bootstrap,
	}

	clust, err := cluster.NewEtcdCluster(clusterCfg, store, nullLogger)
	if err != nil {
		store.Close()
		return fmt.Errorf("failed to create cluster: %w", err)
	}
	b := broker.NewBroker(store, clust, nullLogger, nil, nil, nil, nil, config.SessionConfig{})

	// Create log-backed queue manager for queue/stream integration paths.
	queueStore, err := logStorage.NewAdapter(fmt.Sprintf("%s/queue", node.DataDir), logStorage.DefaultAdapterConfig())
	if err != nil {
		return fmt.Errorf("failed to create queue log storage: %w", err)
	}
	queueCfg := queue.DefaultConfig()
	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		node.recordQueueDelivery(clientID, msg)
		return b.DeliverToSessionByID(ctx, clientID, msg)
	}
	qm := queue.NewManager(queueStore, queueStore, deliverFn, queueCfg, nullLogger, clust)

	// Wire broker as message handler (includes session management)
	clust.SetMessageHandler(b)

	// Start cluster (begins leader election, subscription cache, etc.)
	if err := clust.Start(); err != nil {
		return fmt.Errorf("failed to start cluster: %w", err)
	}

	if err := b.SetQueueManager(qm); err != nil {
		_ = queueStore.Close()
		return fmt.Errorf("failed to set queue manager: %w", err)
	}
	clust.SetQueueHandler(qm)
	node.queueStore = queueStore

	// Create and start TCP server
	tcpCfg := tcp.Config{
		Address: node.TCPAddr,
		Logger:  nullLogger,
	}
	tcpServer := tcp.New(tcpCfg, b)

	// Start TCP server in background
	go func() {
		if err := tcpServer.Listen(node.ctx); err != nil && err != context.Canceled {
			tc.t.Logf("TCP server error on %s: %v", node.ID, err)
		}
		close(node.tcpStopped)
	}()

	// Give TCP server time to start
	time.Sleep(100 * time.Millisecond)

	node.Broker = b
	node.Cluster = clust
	node.TCPServer = tcpServer

	tc.t.Logf("Started node %s (TCP:%d, gRPC:%d, etcd:%d, peer:%d)",
		node.ID, node.Port, node.GRPCPort, node.EtcdPort, node.PeerPort)

	return nil
}

// Stop stops all nodes in the cluster and cleans up.
func (tc *TestCluster) Stop() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.stopped {
		return
	}

	tc.stopAllNodes()
	tc.stopped = true
}

func (tc *TestCluster) stopAllNodes() {
	for _, node := range tc.Nodes {
		if node != nil {
			tc.stopNode(node)
		}
	}
}

func (tc *TestCluster) stopNode(node *TestNode) {
	// Cancel context to stop TCP server
	if node.cancel != nil {
		node.cancel()
	}

	// Wait for TCP server to stop
	if node.tcpStopped != nil {
		select {
		case <-node.tcpStopped:
		case <-time.After(2 * time.Second):
			tc.t.Logf("TCP server stop timeout on %s", node.ID)
		}
	}

	// Stop broker first so queue/session cleanup finishes before etcd client closes.
	if node.Broker != nil {
		node.Broker.Close()
	}

	// Stop cluster after broker/queue shutdown.
	if node.Cluster != nil {
		node.Cluster.Stop()
	}

	if node.queueStore != nil {
		_ = node.queueStore.Close()
		node.queueStore = nil
	}

	// Clean up data directory
	if node.DataDir != "" {
		os.RemoveAll(node.DataDir)
	}

	node.queueDeliveriesMu.Lock()
	node.queueDeliveries = nil
	node.queueDeliveriesMu.Unlock()

	tc.t.Logf("Stopped node %s", node.ID)
}

// GetNode returns a node by ID.
func (tc *TestCluster) GetNode(id string) *TestNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	for _, node := range tc.Nodes {
		if node.ID == id {
			return node
		}
	}
	return nil
}

// GetNodeByIndex returns a node by index.
func (tc *TestCluster) GetNodeByIndex(index int) *TestNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if index >= 0 && index < len(tc.Nodes) {
		return tc.Nodes[index]
	}
	return nil
}

// KillNode stops a node ungracefully (for testing failover).
func (tc *TestCluster) KillNode(id string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	node := tc.GetNode(id)
	if node == nil {
		return fmt.Errorf("node %s not found", id)
	}

	tc.stopNode(node)
	return nil
}

// WaitForClusterReady waits for the cluster to be ready.
// This means etcd cluster is formed and at least one leader is elected.
func (tc *TestCluster) WaitForClusterReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Check if we can get cluster membership from any node
		for _, node := range tc.Nodes {
			if node.Cluster != nil {
				nodes := node.Cluster.Nodes()
				if len(nodes) == len(tc.Nodes) {
					if leader := tc.GetLeader(); leader != nil {
						tc.t.Logf("Cluster ready: %d nodes, leader: %s", len(nodes), leader.ID)
						return nil
					}
				}
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("cluster did not become ready within %v", timeout)
}

// GetLeader returns the current leader node (if any).
func (tc *TestCluster) GetLeader() *TestNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	for _, node := range tc.Nodes {
		if node.Cluster != nil && node.Cluster.IsLeader() {
			return node
		}
	}
	return nil
}

// WaitForLeader waits for a leader to be elected.
func (tc *TestCluster) WaitForLeader(timeout time.Duration) (*TestNode, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leader := tc.GetLeader(); leader != nil {
			tc.t.Logf("Leader elected: %s", leader.ID)
			return leader, nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	return nil, fmt.Errorf("no leader elected within %v", timeout)
}
