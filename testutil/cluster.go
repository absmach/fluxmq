// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/server/tcp"
	"github.com/absmach/mqtt/storage/badger"
	"github.com/stretchr/testify/require"
)

// TestCluster represents a cluster of test nodes.
type TestCluster struct {
	t       *testing.T
	Nodes   []*TestNode
	mu      sync.RWMutex
	stopped bool
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

	// Base ports for allocation
	// Make sure etcd client and peer port ranges don't overlap!
	baseTCPPort := 10883  // MQTT TCP: 10883, 10884, 10885, ...
	baseGRPCPort := 19000 // gRPC: 19000, 19001, 19002, ...
	baseEtcdPort := 12379 // etcd client: 12379, 12380, 12381, ...
	basePeerPort := 12390 // etcd peer: 12390, 12391, 12392, ...

	// Build initial cluster string for etcd
	initialCluster := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		peerAddr := fmt.Sprintf("127.0.0.1:%d", basePeerPort+i)
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
		node := tc.createNode(i, baseTCPPort+i, baseGRPCPort+i, baseEtcdPort+i, basePeerPort+i, initialClusterStr)
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

	clust, err := cluster.NewEtcdCluster(clusterCfg)
	if err != nil {
		store.Close()
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	// Create broker with null logger to reduce test noise
	nullLogger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := broker.NewBroker(store, clust, nullLogger, nil, nil)

	// Wire broker as message handler (includes session management)
	clust.SetMessageHandler(b)

	// Start cluster (begins leader election, subscription cache, etc.)
	if err := clust.Start(); err != nil {
		return fmt.Errorf("failed to start cluster: %w", err)
	}

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

	// Stop cluster first
	if node.Cluster != nil {
		node.Cluster.Stop()
	}

	// Close broker
	if node.Broker != nil {
		node.Broker.Close()
	}

	// Clean up data directory
	if node.DataDir != "" {
		os.RemoveAll(node.DataDir)
	}

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
