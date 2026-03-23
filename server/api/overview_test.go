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
	mqtt "github.com/absmach/fluxmq/mqtt"
	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
)

const testNodeID = "node-1"

func TestOverviewCombinesStatsClusterSessions(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))

	// Create two sessions, connect both
	for _, id := range []string{"client-a", "client-b"} {
		s, _, err := b.CreateSession(id, mqtt.ProtocolV5, session.Options{
			CleanStart:     false,
			KeepAlive:      30 * time.Second,
			ReceiveMaximum: 10,
		})
		if err != nil {
			t.Fatalf("create session %s: %v", id, err)
		}
		if err := s.Connect(&testConn{}); err != nil {
			t.Fatalf("connect session %s: %v", id, err)
		}
	}

	b.Stats().IncrementConnections()
	b.Stats().IncrementConnections()
	b.Stats().IncrementPublishReceived()

	stub := &clusterStub{
		nodeID: testNodeID,
		leader: true,
		nodes: []cluster.NodeInfo{
			{ID: testNodeID, Address: "10.0.0.1:7946", Healthy: true, Leader: true, Uptime: time.Hour},
		},
	}
	srv := New(Config{}, b, nil, stub, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/overview", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp overviewResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.NodeID != testNodeID {
		t.Fatalf("expected node_id %q, got %q", testNodeID, resp.NodeID)
	}
	if !resp.ClusterMode {
		t.Fatal("expected cluster_mode true")
	}
	if resp.Sessions.Connected != 2 {
		t.Fatalf("expected 2 connected sessions, got %d", resp.Sessions.Connected)
	}
	if resp.Sessions.Total != 2 {
		t.Fatalf("expected 2 total sessions, got %d", resp.Sessions.Total)
	}
	if resp.Stats.Messages.Received != 1 {
		t.Fatalf("expected messages received 1, got %d", resp.Stats.Messages.Received)
	}
	if resp.Stats.ByProtocol.MQTT == nil {
		t.Fatal("expected mqtt in by_protocol")
	}
	if resp.Stats.ByProtocol.MQTT.Messages.PublishReceived != 1 {
		t.Fatalf("expected mqtt publish_received 1, got %d", resp.Stats.ByProtocol.MQTT.Messages.PublishReceived)
	}
	if len(resp.Cluster.Nodes) != 1 {
		t.Fatalf("expected 1 cluster node, got %d", len(resp.Cluster.Nodes))
	}
	if resp.UptimeSeconds <= 0 {
		t.Fatal("expected positive uptime")
	}
}

func TestOverviewNilBrokersReturns503(t *testing.T) {
	srv := New(Config{}, nil, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/overview", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestOverviewRejectsPost(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/overview", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}
