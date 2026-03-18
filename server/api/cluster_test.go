// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage/memory"
)

type clusterStub struct {
	cluster.Cluster
	nodeID string
	leader bool
	nodes  []cluster.NodeInfo
}

func (c *clusterStub) NodeID() string           { return c.nodeID }
func (c *clusterStub) IsLeader() bool            { return c.leader }
func (c *clusterStub) Nodes() []cluster.NodeInfo { return c.nodes }

func TestClusterNilClusterReturnsSingleNode(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp clusterResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.ClusterMode {
		t.Fatal("expected cluster_mode false for nil cluster")
	}
	if resp.NodeID != "single-node" {
		t.Fatalf("expected node_id 'single-node', got %q", resp.NodeID)
	}
	if !resp.IsLeader {
		t.Fatal("expected is_leader true for single-node")
	}
	if len(resp.Nodes) != 0 {
		t.Fatalf("expected empty nodes, got %d", len(resp.Nodes))
	}
}

func TestClusterWithStub(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))

	stub := &clusterStub{
		nodeID: "node-1",
		leader: true,
		nodes: []cluster.NodeInfo{
			{ID: "node-1", Address: "10.0.0.1:7946", Healthy: true, Leader: true, Uptime: 24 * time.Hour},
			{ID: "node-2", Address: "10.0.0.2:7946", Healthy: true, Leader: false, Uptime: 12 * time.Hour},
		},
	}
	srv := New(Config{}, b, nil, stub, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp clusterResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp.ClusterMode {
		t.Fatal("expected cluster_mode true")
	}
	if resp.NodeID != "node-1" {
		t.Fatalf("expected node_id 'node-1', got %q", resp.NodeID)
	}
	if !resp.IsLeader {
		t.Fatal("expected is_leader true")
	}
	if len(resp.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(resp.Nodes))
	}
	if resp.Nodes[0].ID != "node-1" || !resp.Nodes[0].Leader {
		t.Fatalf("unexpected node-1 data: %+v", resp.Nodes[0])
	}
	if resp.Nodes[1].ID != "node-2" || resp.Nodes[1].Leader {
		t.Fatalf("unexpected node-2 data: %+v", resp.Nodes[1])
	}
}

func TestClusterNilBrokerReturns503(t *testing.T) {
	srv := New(Config{}, nil, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestClusterRejectsPost(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/cluster", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}
