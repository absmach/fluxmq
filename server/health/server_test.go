// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

// mockCluster implements cluster.Cluster interface for testing.
type mockCluster struct {
	nodeID   string
	isLeader bool
}

func (m *mockCluster) NodeID() string {
	return m.nodeID
}

func (m *mockCluster) IsLeader() bool {
	return m.isLeader
}

func (m *mockCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	return nil
}

func (m *mockCluster) AcquirePartition(ctx context.Context, queueName string, partitionID int, nodeID string) error {
	return nil
}

func (m *mockCluster) ReleasePartition(ctx context.Context, queueName string, partitionID int) error {
	return nil
}

func (m *mockCluster) EnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	return "", nil
}

func (m *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64, partitionID int) error {
	return nil
}

func (m *mockCluster) ReleaseSession(ctx context.Context, clientID string) error {
	return nil
}

func (m *mockCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	return "", false, nil
}

func (m *mockCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error {
	return nil
}

func (m *mockCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error {
	return nil
}

func (m *mockCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error) {
	return nil, nil
}

func (m *mockCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error) {
	return nil, nil
}

func (m *mockCluster) Retained() storage.RetainedStore {
	return nil
}

func (m *mockCluster) Wills() storage.WillStore {
	return nil
}

func (m *mockCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	return nil
}

func (m *mockCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (m *mockCluster) WaitForLeader(ctx context.Context) error {
	return nil
}

func (m *mockCluster) Start() error {
	return nil
}

func (m *mockCluster) Stop() error {
	return nil
}

func (m *mockCluster) Nodes() []cluster.NodeInfo {
	return nil
}

func TestServerStartStop(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, b, nil, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestHealthEndpoint(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, b, nil, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr()

	tests := []struct {
		name           string
		method         string
		expectedStatus int
		expectedBody   HealthResponse
	}{
		{
			name:           "GET request returns healthy",
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedBody:   HealthResponse{Status: "healthy"},
		},
		{
			name:           "POST request not allowed",
			method:         http.MethodPost,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "PUT request not allowed",
			method:         http.MethodPut,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://"+addr+"/health", nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			if tt.expectedStatus == http.StatusOK {
				var response HealthResponse
				if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}

				if response.Status != tt.expectedBody.Status {
					t.Errorf("expected status %q, got %q", tt.expectedBody.Status, response.Status)
				}
			}
		})
	}
}

func TestReadyEndpoint(t *testing.T) {
	tests := []struct {
		name           string
		broker         *broker.Broker
		cluster        cluster.Cluster
		method         string
		expectedStatus int
		expectedReady  bool
		expectedReason string
	}{
		{
			name:           "broker nil - not ready",
			broker:         nil,
			cluster:        nil,
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "broker not initialized",
		},
		{
			name:           "single node mode - ready",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil),
			cluster:        nil,
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
		},
		{
			name:           "cluster not initialized - not ready",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil),
			cluster:        &mockCluster{nodeID: ""},
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "cluster not initialized",
		},
		{
			name:           "cluster initialized - ready",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil),
			cluster:        &mockCluster{nodeID: "node-1", isLeader: true},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
		},
		{
			name:           "POST request not allowed",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil),
			cluster:        nil,
			method:         http.MethodPost,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.broker != nil {
				defer tt.broker.Close()
			}

			cfg := Config{
				Address:         "localhost:0",
				ShutdownTimeout: 1 * time.Second,
			}

			server := New(cfg, tt.broker, tt.cluster, slog.Default())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go server.Listen(ctx)
			time.Sleep(100 * time.Millisecond)

			addr := server.Addr()

			req, err := http.NewRequest(tt.method, "http://"+addr+"/ready", nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			if tt.expectedStatus == http.StatusOK || tt.expectedStatus == http.StatusServiceUnavailable {
				var response ReadyResponse
				if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}

				if tt.expectedReady && response.Status != "ready" {
					t.Errorf("expected ready status, got %q", response.Status)
				}

				if !tt.expectedReady && response.Status != "not_ready" {
					t.Errorf("expected not_ready status, got %q", response.Status)
				}

				if tt.expectedReason != "" && response.Details != tt.expectedReason {
					t.Errorf("expected details %q, got %q", tt.expectedReason, response.Details)
				}
			}
		})
	}
}

func TestClusterStatusEndpoint(t *testing.T) {
	tests := []struct {
		name                  string
		cluster               cluster.Cluster
		method                string
		expectedStatus        int
		expectedCluster       bool
		expectedNodeID        string
		expectedIsLeader      bool
		checkMethodNotAllowed bool
	}{
		{
			name:            "single node mode",
			cluster:         nil,
			method:          http.MethodGet,
			expectedStatus:  http.StatusOK,
			expectedCluster: false,
			expectedNodeID:  "single-node",
		},
		{
			name:             "cluster mode - follower",
			cluster:          &mockCluster{nodeID: "node-2", isLeader: false},
			method:           http.MethodGet,
			expectedStatus:   http.StatusOK,
			expectedCluster:  true,
			expectedNodeID:   "node-2",
			expectedIsLeader: false,
		},
		{
			name:             "cluster mode - leader",
			cluster:          &mockCluster{nodeID: "node-1", isLeader: true},
			method:           http.MethodGet,
			expectedStatus:   http.StatusOK,
			expectedCluster:  true,
			expectedNodeID:   "node-1",
			expectedIsLeader: true,
		},
		{
			name:                  "POST request not allowed",
			cluster:               nil,
			method:                http.MethodPost,
			expectedStatus:        http.StatusMethodNotAllowed,
			checkMethodNotAllowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil)
			defer b.Close()

			cfg := Config{
				Address:         "localhost:0",
				ShutdownTimeout: 1 * time.Second,
			}

			server := New(cfg, b, tt.cluster, slog.Default())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go server.Listen(ctx)
			time.Sleep(100 * time.Millisecond)

			addr := server.Addr()

			req, err := http.NewRequest(tt.method, "http://"+addr+"/cluster/status", nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, resp.StatusCode)
			}

			if tt.checkMethodNotAllowed {
				return
			}

			if tt.expectedStatus == http.StatusOK {
				var response ClusterStatusResponse
				if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}

				if response.ClusterMode != tt.expectedCluster {
					t.Errorf("expected cluster mode %v, got %v", tt.expectedCluster, response.ClusterMode)
				}

				if response.NodeID != tt.expectedNodeID {
					t.Errorf("expected node ID %q, got %q", tt.expectedNodeID, response.NodeID)
				}

				if response.IsLeader != tt.expectedIsLeader {
					t.Errorf("expected is_leader %v, got %v", tt.expectedIsLeader, response.IsLeader)
				}

				// Sessions count should be >= 0
				if response.Sessions < 0 {
					t.Errorf("expected non-negative sessions, got %d", response.Sessions)
				}
			}
		})
	}
}

func TestContentTypeHeaders(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, b, nil, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr()

	endpoints := []string{"/health", "/ready", "/cluster/status"}

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			resp, err := http.Get("http://" + addr + endpoint)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
			defer resp.Body.Close()

			contentType := resp.Header.Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("expected Content-Type application/json, got %q", contentType)
			}

			// Verify it's valid JSON
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}

			var data map[string]interface{}
			if err := json.Unmarshal(body, &data); err != nil {
				t.Errorf("response is not valid JSON: %v", err)
			}
		})
	}
}

func TestGracefulShutdown(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 2 * time.Second,
	}

	server := New(cfg, b, nil, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	addr := server.Addr()

	// Make a request while server is running
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("failed to get health before shutdown: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Trigger shutdown
	cancel()

	// Server should stop gracefully
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("shutdown completed with: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop after shutdown timeout")
	}

	// After shutdown, requests should fail
	_, err = http.Get("http://" + addr + "/health")
	if err == nil {
		t.Error("expected request to fail after shutdown")
	}
}
