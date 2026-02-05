// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/broker"
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

func (m *mockCluster) ReleaseSession(ctx context.Context, clientID string) error {
	return nil
}

func (m *mockCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	return "", false, nil
}

func (m *mockCluster) WatchSessionOwner(ctx context.Context, clientID string) <-chan cluster.OwnershipChange {
	return nil
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

func (m *mockCluster) RegisterQueueConsumer(ctx context.Context, info *cluster.QueueConsumerInfo) error {
	return nil
}

func (m *mockCluster) UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return nil
}

func (m *mockCluster) ListQueueConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (m *mockCluster) ListQueueConsumersByGroup(ctx context.Context, queueName, groupID string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (m *mockCluster) ListAllQueueConsumers(ctx context.Context) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (m *mockCluster) ForwardQueuePublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string, forwardToLeader bool) error {
	return nil
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

func (m *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64) error {
	return nil
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

func TestAddrWithoutListener(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{})
	defer b.Close()

	server := New(Config{}, b, nil, slog.Default())
	if server.Addr() != "" {
		t.Fatalf("expected empty address before listen, got %q", server.Addr())
	}
}

func TestHealthEndpoint(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{})
	defer b.Close()

	server := New(Config{}, b, nil, slog.Default())

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
			req := httptest.NewRequest(tt.method, "http://test/health", nil)
			rec := httptest.NewRecorder()

			server.handleHealth(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			if tt.expectedStatus == http.StatusOK {
				var response HealthResponse
				if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
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
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}),
			cluster:        nil,
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
		},
		{
			name:           "cluster not initialized - not ready",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}),
			cluster:        &mockCluster{nodeID: ""},
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "cluster not initialized",
		},
		{
			name:           "cluster initialized - ready",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}),
			cluster:        &mockCluster{nodeID: "node-1", isLeader: true},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
		},
		{
			name:           "POST request not allowed",
			broker:         broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}),
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

			server := New(Config{}, tt.broker, tt.cluster, slog.Default())

			req := httptest.NewRequest(tt.method, "http://test/ready", nil)
			rec := httptest.NewRecorder()

			server.handleReady(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			if tt.expectedStatus == http.StatusOK || tt.expectedStatus == http.StatusServiceUnavailable {
				var response ReadyResponse
				if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
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
			b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{})
			defer b.Close()

			server := New(Config{}, b, tt.cluster, slog.Default())

			req := httptest.NewRequest(tt.method, "http://test/cluster/status", nil)
			rec := httptest.NewRecorder()

			server.handleClusterStatus(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			if tt.checkMethodNotAllowed {
				return
			}

			if tt.expectedStatus == http.StatusOK {
				var response ClusterStatusResponse
				if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
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
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{})
	defer b.Close()

	server := New(Config{}, b, nil, slog.Default())

	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{name: "/health", handler: server.handleHealth},
		{name: "/ready", handler: server.handleReady},
		{name: "/cluster/status", handler: server.handleClusterStatus},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://test"+tt.name, nil)
			rec := httptest.NewRecorder()

			tt.handler(rec, req)

			contentType := rec.Header().Get("Content-Type")
			if contentType != "application/json" {
				t.Errorf("expected Content-Type application/json, got %q", contentType)
			}

			body, err := io.ReadAll(rec.Body)
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
