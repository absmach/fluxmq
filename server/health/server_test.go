// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/broker"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

// --- mocks ---

type mockCluster struct {
	nodeID   string
	isLeader bool
	nodes    []cluster.NodeInfo
}

func (m *mockCluster) NodeID() string { return m.nodeID }
func (m *mockCluster) IsLeader() bool { return m.isLeader }
func (m *mockCluster) Start() error   { return nil }
func (m *mockCluster) Stop() error    { return nil }
func (m *mockCluster) Nodes() []cluster.NodeInfo {
	if m.nodes != nil {
		return m.nodes
	}
	return nil
}
func (m *mockCluster) WaitForLeader(ctx context.Context) error { return nil }
func (m *mockCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	return nil
}
func (m *mockCluster) ReleaseSession(ctx context.Context, clientID string) error { return nil }
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

func (m *mockCluster) RemoveAllSubscriptions(ctx context.Context, clientID string) error {
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

func (m *mockCluster) ForwardGroupOp(ctx context.Context, nodeID, queueName string, opData []byte) error {
	return nil
}
func (m *mockCluster) Retained() storage.RetainedStore { return nil }
func (m *mockCluster) Wills() storage.WillStore        { return nil }
func (m *mockCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	return nil
}

func (m *mockCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (m *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName string, msg *cluster.QueueMessage) error {
	return nil
}

type mockStore struct {
	pingErr error
}

func (m *mockStore) Messages() storage.MessageStore           { return nil }
func (m *mockStore) Sessions() storage.SessionStore           { return nil }
func (m *mockStore) Subscriptions() storage.SubscriptionStore { return nil }
func (m *mockStore) Retained() storage.RetainedStore          { return nil }
func (m *mockStore) Wills() storage.WillStore                 { return nil }
func (m *mockStore) Close() error                             { return nil }
func (m *mockStore) Ping() error                              { return m.pingErr }

// --- helpers ---

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() })
	return b
}

func doRequest(handler http.HandlerFunc, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "http://test"+path, nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func decodeReady(t *testing.T, rec *httptest.ResponseRecorder) ReadyResponse {
	t.Helper()
	var resp ReadyResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode ReadyResponse: %v", err)
	}
	return resp
}

// --- tests ---

func TestAddrWithoutListener(t *testing.T) {
	server := New(Config{}, newTestBroker(t), nil, nil, slog.Default())
	if server.Addr() != "" {
		t.Fatalf("expected empty address before listen, got %q", server.Addr())
	}
}

func TestHealthEndpoint(t *testing.T) {
	server := New(Config{}, newTestBroker(t), nil, nil, slog.Default())

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(server.handleHealth, tt.method, "/health")
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
		store          storage.Store
		method         string
		expectedStatus int
		expectedReady  bool
		expectedMode   string
		expectedReason string
		checkName      string
		checkStatus    string
		absentCheck    string
	}{
		{
			name:           "broker nil - not ready",
			broker:         nil,
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "broker not initialized",
			checkName:      "broker",
			checkStatus:    StatusDown,
		},
		{
			name:           "nil store - not ready",
			broker:         broker.NewBroker(nil, nil),
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "storage not initialized",
			checkName:      "storage",
			checkStatus:    StatusDown,
		},
		{
			name:           "storage healthy - ready nominal",
			broker:         broker.NewBroker(nil, nil),
			store:          &mockStore{},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
			expectedMode:   ModeNominal,
			checkName:      "storage",
			checkStatus:    StatusUp,
			absentCheck:    "cluster",
		},
		{
			name:           "noop cluster treated as single-node mode",
			broker:         broker.NewBroker(nil, nil),
			cluster:        cluster.NewNoopCluster("single-node"),
			store:          &mockStore{},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
			expectedMode:   ModeNominal,
			checkName:      "storage",
			checkStatus:    StatusUp,
			absentCheck:    "cluster",
		},
		{
			name:           "storage down - not ready",
			broker:         broker.NewBroker(nil, nil),
			store:          &mockStore{pingErr: errors.New("db closed")},
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "storage unavailable",
			checkName:      "storage",
			checkStatus:    StatusDown,
		},
		{
			name:           "cluster not initialized - not ready",
			broker:         broker.NewBroker(nil, nil),
			cluster:        &mockCluster{nodeID: ""},
			store:          &mockStore{},
			method:         http.MethodGet,
			expectedStatus: http.StatusServiceUnavailable,
			expectedReady:  false,
			expectedReason: "cluster not initialized",
			checkName:      "cluster",
			checkStatus:    StatusDown,
		},
		{
			name:   "cluster all peers healthy - ready nominal",
			broker: broker.NewBroker(nil, nil),
			cluster: &mockCluster{
				nodeID:   "node-1",
				isLeader: true,
				nodes: []cluster.NodeInfo{
					{ID: "node-1", Healthy: true},
					{ID: "node-2", Healthy: true},
					{ID: "node-3", Healthy: true},
				},
			},
			store:          &mockStore{},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
			expectedMode:   ModeNominal,
			checkName:      "cluster",
			checkStatus:    StatusUp,
		},
		{
			name:   "cluster peer unreachable - ready degraded",
			broker: broker.NewBroker(nil, nil),
			cluster: &mockCluster{
				nodeID:   "node-1",
				isLeader: true,
				nodes: []cluster.NodeInfo{
					{ID: "node-1", Healthy: true},
					{ID: "node-2", Healthy: true},
					{ID: "node-3", Healthy: false},
				},
			},
			store:          &mockStore{},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
			expectedMode:   ModeDegraded,
			checkName:      "cluster",
			checkStatus:    StatusUp,
		},
		{
			name:   "cluster all peers unreachable - ready degraded",
			broker: broker.NewBroker(nil, nil),
			cluster: &mockCluster{
				nodeID:   "node-1",
				isLeader: true,
				nodes: []cluster.NodeInfo{
					{ID: "node-1", Healthy: true},
					{ID: "node-2", Healthy: false},
					{ID: "node-3", Healthy: false},
				},
			},
			store:          &mockStore{},
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			expectedReady:  true,
			expectedMode:   ModeDegraded,
			checkName:      "cluster",
			checkStatus:    StatusUp,
		},
		{
			name:           "POST request not allowed",
			broker:         broker.NewBroker(nil, nil),
			method:         http.MethodPost,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.broker != nil {
				defer tt.broker.Close()
			}

			server := New(Config{}, tt.broker, tt.cluster, tt.store, slog.Default())
			rec := doRequest(server.handleReady, tt.method, "/ready")

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			if tt.expectedStatus == http.StatusMethodNotAllowed {
				return
			}

			resp := decodeReady(t, rec)

			if tt.expectedReady && resp.Status != "ready" {
				t.Errorf("expected ready status, got %q", resp.Status)
			}
			if !tt.expectedReady && resp.Status != "not_ready" {
				t.Errorf("expected not_ready status, got %q", resp.Status)
			}
			if tt.expectedMode != "" && resp.Mode != tt.expectedMode {
				t.Errorf("expected mode %q, got %q", tt.expectedMode, resp.Mode)
			}
			if tt.expectedReason != "" && resp.Details != tt.expectedReason {
				t.Errorf("expected details %q, got %q", tt.expectedReason, resp.Details)
			}
			if tt.checkName != "" {
				check, ok := resp.Checks[tt.checkName]
				if !ok {
					t.Fatalf("expected check %q in response, got %v", tt.checkName, resp.Checks)
				}
				if check.Status != tt.checkStatus {
					t.Errorf("expected check %q status %q, got %q", tt.checkName, tt.checkStatus, check.Status)
				}
			}
			if tt.absentCheck != "" {
				if _, ok := resp.Checks[tt.absentCheck]; ok {
					t.Errorf("expected check %q to be omitted, got %v", tt.absentCheck, resp.Checks)
				}
			}
		})
	}
}

func TestReadyDegradedDetails(t *testing.T) {
	b := newTestBroker(t)
	cl := &mockCluster{
		nodeID:   "node-1",
		isLeader: true,
		nodes: []cluster.NodeInfo{
			{ID: "node-1", Healthy: true},
			{ID: "node-2", Healthy: true},
			{ID: "node-3", Healthy: false},
		},
	}

	server := New(Config{}, b, cl, &mockStore{}, slog.Default())
	rec := doRequest(server.handleReady, http.MethodGet, "/ready")

	resp := decodeReady(t, rec)
	if resp.Mode != ModeDegraded {
		t.Fatalf("expected degraded mode, got %q", resp.Mode)
	}

	check := resp.Checks["cluster"]
	if check == nil {
		t.Fatal("expected cluster check in response")
	}
	if check.Details != "1/2 peers reachable" {
		t.Errorf("expected '1/2 peers reachable', got %q", check.Details)
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
			name:            "noop cluster mode is reported as single node",
			cluster:         cluster.NewNoopCluster("node-1"),
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
			b := newTestBroker(t)
			server := New(Config{}, b, tt.cluster, nil, slog.Default())
			rec := doRequest(server.handleClusterStatus, tt.method, "/cluster/status")

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
				if response.Sessions < 0 {
					t.Errorf("expected non-negative sessions, got %d", response.Sessions)
				}
			}
		})
	}
}

func TestContentTypeHeaders(t *testing.T) {
	b := newTestBroker(t)
	server := New(Config{}, b, nil, nil, slog.Default())

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
			rec := doRequest(tt.handler, http.MethodGet, tt.name)

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

func TestCountPeers(t *testing.T) {
	tests := []struct {
		name            string
		selfID          string
		nodes           []cluster.NodeInfo
		expectedTotal   int
		expectedHealthy int
	}{
		{
			name:            "no peers",
			selfID:          "node-1",
			nodes:           []cluster.NodeInfo{{ID: "node-1", Healthy: true}},
			expectedTotal:   0,
			expectedHealthy: 0,
		},
		{
			name:   "all peers healthy",
			selfID: "node-1",
			nodes: []cluster.NodeInfo{
				{ID: "node-1", Healthy: true},
				{ID: "node-2", Healthy: true},
				{ID: "node-3", Healthy: true},
			},
			expectedTotal:   2,
			expectedHealthy: 2,
		},
		{
			name:   "one peer down",
			selfID: "node-1",
			nodes: []cluster.NodeInfo{
				{ID: "node-1", Healthy: true},
				{ID: "node-2", Healthy: true},
				{ID: "node-3", Healthy: false},
			},
			expectedTotal:   2,
			expectedHealthy: 1,
		},
		{
			name:   "all peers down",
			selfID: "node-1",
			nodes: []cluster.NodeInfo{
				{ID: "node-1", Healthy: true},
				{ID: "node-2", Healthy: false},
				{ID: "node-3", Healthy: false},
			},
			expectedTotal:   2,
			expectedHealthy: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, healthy := countPeers(tt.selfID, tt.nodes)
			if total != tt.expectedTotal {
				t.Errorf("expected total %d, got %d", tt.expectedTotal, total)
			}
			if healthy != tt.expectedHealthy {
				t.Errorf("expected healthy %d, got %d", tt.expectedHealthy, healthy)
			}
		})
	}
}
