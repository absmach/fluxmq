// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	corebroker "github.com/absmach/fluxmq/broker"
	mqtt "github.com/absmach/fluxmq/mqtt"
	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/absmach/fluxmq/storage/messages"
)

const testTopicFilter = "devices/+/events"

type testConn struct{}

func (c *testConn) Read(b []byte) (int, error)                 { return 0, io.EOF }
func (c *testConn) Write(b []byte) (int, error)                { return len(b), nil }
func (c *testConn) Close() error                               { return nil }
func (c *testConn) LocalAddr() net.Addr                        { return testAddr("local") }
func (c *testConn) RemoteAddr() net.Addr                       { return testAddr("remote") }
func (c *testConn) SetDeadline(time.Time) error                { return nil }
func (c *testConn) SetReadDeadline(time.Time) error            { return nil }
func (c *testConn) SetWriteDeadline(time.Time) error           { return nil }
func (c *testConn) ReadPacket() (packets.ControlPacket, error) { return nil, io.EOF }
func (c *testConn) WritePacket(packets.ControlPacket) error    { return nil }
func (c *testConn) WriteControlPacket(packets.ControlPacket, func()) error {
	return nil
}
func (c *testConn) WriteDataPacket(packets.ControlPacket, func()) error { return nil }
func (c *testConn) TryWriteDataPacket(packets.ControlPacket, func()) error {
	return nil
}
func (c *testConn) SetKeepAlive(time.Duration) error { return nil }
func (c *testConn) SetOnDisconnect(func(bool))       {}
func (c *testConn) Touch()                           {}

type testAddr string

func (a testAddr) Network() string { return "tcp" }
func (a testAddr) String() string  { return string(a) }

func TestSessionsListEndpointFiltersAndPaginates(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions?state=connected&prefix=alpha&limit=1", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSessionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.Sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(resp.Sessions))
	}
	if resp.Sessions[0].ClientID != "alpha-live" {
		t.Fatalf("expected alpha-live, got %q", resp.Sessions[0].ClientID)
	}
	if !resp.Sessions[0].Connected {
		t.Fatalf("expected connected session")
	}
	if resp.NextPageToken != "" {
		t.Fatalf("expected empty next_page_token, got %q", resp.NextPageToken)
	}
}

func TestSessionDetailEndpointSupportsEscapedClientID(t *testing.T) {
	srv := newTestAPIServer(t)

	clientID := "tenant/a/client-1"
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+url.PathEscape(clientID), nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp sessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ClientID != clientID {
		t.Fatalf("expected client_id %q, got %q", clientID, resp.ClientID)
	}
	if resp.Protocol != "mqtt5" {
		t.Fatalf("expected mqtt5 protocol, got %q", resp.Protocol)
	}
	if resp.SubscriptionCount != 1 {
		t.Fatalf("expected 1 subscription, got %d", resp.SubscriptionCount)
	}
	if len(resp.Subscriptions) != 1 || resp.Subscriptions[0].Filter != testTopicFilter {
		t.Fatalf("unexpected subscriptions: %#v", resp.Subscriptions)
	}
}

func TestSessionDetailEndpointDetectsAMQPProtocolFromClientIDPrefix(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	clientID := corebroker.PrefixedAMQP091ClientID("conn-1")
	createSessionWithVersion(t, b, store, clientID, 0, true, "amqp/one")

	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+url.PathEscape(clientID), nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp sessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Protocol != "amqp0.9.1" {
		t.Fatalf("expected amqp0.9.1 protocol, got %q", resp.Protocol)
	}
}

func TestAMQPSessionResponseIncludesConnectionNameAndSubscriptions(t *testing.T) {
	resp := amqpSessionResponse(
		corebroker.PrefixedAMQP091ClientID("conn-1"),
		"orders-consumer",
		[]amqpbroker.SubscriptionSnapshot{
			{
				ClientID: corebroker.PrefixedAMQP091ClientID("conn-1"),
				Filter:   "orders.*",
				QoS:      1,
			},
		},
		true,
	)

	if resp.ConnectionName != "orders-consumer" {
		t.Fatalf("expected connection name orders-consumer, got %q", resp.ConnectionName)
	}
	if resp.SubscriptionCount != 1 {
		t.Fatalf("expected subscription count 1, got %d", resp.SubscriptionCount)
	}
	if len(resp.Subscriptions) != 1 || resp.Subscriptions[0].Filter != "orders.*" {
		t.Fatalf("unexpected subscriptions: %#v", resp.Subscriptions)
	}
}

func TestSessionsListEndpointRejectsInvalidState(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions?state=broken", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestSessionsListEndpointRejectsPost(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sessions", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestSessionDetailEndpointRejectsPost(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sessions/alpha-live", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestSessionDetailEndpointNotFound(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/nonexistent", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestSessionsTrailingSlashDelegatesToList(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSessionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Sessions) != 3 {
		t.Fatalf("expected 3 sessions from trailing-slash list, got %d", len(resp.Sessions))
	}
}

func TestSessionsNilBrokerReturnsServiceUnavailable(t *testing.T) {
	srv := New(Config{}, nil, nil, nil, nil, nil, nil, slog.Default())

	tests := []struct {
		name string
		path string
	}{
		{"list", "/api/v1/sessions"},
		{"detail", "/api/v1/sessions/some-client"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			srv.httpServer.Handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rec.Code)
			}
		})
	}
}

func TestSessionsAMQPOnlyListReturnsOK(t *testing.T) {
	srv := New(
		Config{},
		nil,
		amqpbroker.New(nil, slog.Default()),
		nil,
		nil,
		nil,
		nil,
		slog.Default(),
	)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSessionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Sessions) != 0 {
		t.Fatalf("expected no sessions, got %d", len(resp.Sessions))
	}
}

func TestSessionsAMQPOnlyDetailMissingReturnsNotFound(t *testing.T) {
	srv := New(
		Config{},
		nil,
		amqpbroker.New(nil, slog.Default()),
		nil,
		nil,
		nil,
		nil,
		slog.Default(),
	)

	clientID := corebroker.PrefixedAMQP091ClientID("missing-conn")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+url.PathEscape(clientID), nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestSessionsListPaginationWithNextPageToken(t *testing.T) {
	srv := newTestAPIServer(t)

	// Page 1
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=2", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("page 1: expected 200, got %d", rec.Code)
	}

	var page1 listSessionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&page1); err != nil {
		t.Fatalf("page 1: decode: %v", err)
	}
	if len(page1.Sessions) != 2 {
		t.Fatalf("page 1: expected 2 sessions, got %d", len(page1.Sessions))
	}
	if page1.NextPageToken == "" {
		t.Fatal("page 1: expected non-empty next_page_token")
	}

	// Page 2
	req = httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=2&page_token="+page1.NextPageToken, nil)
	rec = httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("page 2: expected 200, got %d", rec.Code)
	}

	var page2 listSessionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&page2); err != nil {
		t.Fatalf("page 2: decode: %v", err)
	}
	if len(page2.Sessions) != 1 {
		t.Fatalf("page 2: expected 1 session, got %d", len(page2.Sessions))
	}
	if page2.NextPageToken != "" {
		t.Fatalf("page 2: expected empty next_page_token, got %q", page2.NextPageToken)
	}

	// Verify no duplicates across pages
	seen := make(map[string]bool)
	for _, s := range page1.Sessions {
		seen[s.ClientID] = true
	}
	for _, s := range page2.Sessions {
		if seen[s.ClientID] {
			t.Fatalf("duplicate client %q across pages", s.ClientID)
		}
	}
}

func TestSessionDetailDisconnectedSession(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/bravo-offline", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp sessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Connected {
		t.Fatal("expected disconnected session")
	}
	if resp.State != "disconnected" {
		t.Fatalf("expected state 'disconnected', got %q", resp.State)
	}
	// Inflight and offline queue may have been drained by the disconnect
	// callback depending on timing — we only assert the state fields here.
}

func TestSessionsListRejectsInvalidLimit(t *testing.T) {
	srv := newTestAPIServer(t)

	tests := []struct {
		name  string
		query string
	}{
		{"negative", "?limit=-1"},
		{"non-numeric", "?limit=abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions"+tt.query, nil)
			rec := httptest.NewRecorder()
			srv.httpServer.Handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
		})
	}
}

func newTestAPIServer(t *testing.T) *Server {
	t.Helper()

	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))

	createSession(t, b, store, "alpha-live", true, "alpha/one")
	createSession(t, b, store, "bravo-offline", false, "bravo/one")
	createSession(t, b, store, "tenant/a/client-1", true, testTopicFilter)

	return New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())
}

func createSession(t *testing.T, b *mqttbroker.Broker, store *memory.Store, clientID string, connected bool, filter string) {
	createSessionWithVersion(t, b, store, clientID, byte(mqtt.ProtocolV5), connected, filter)
}

func createSessionWithVersion(t *testing.T, b *mqttbroker.Broker, store *memory.Store, clientID string, version byte, connected bool, filter string) {
	t.Helper()

	s, _, err := b.CreateSession(clientID, version, session.Options{
		CleanStart:     false,
		KeepAlive:      30 * time.Second,
		ReceiveMaximum: 10,
	})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if err := store.Subscriptions().Add(&storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      1,
	}); err != nil {
		t.Fatalf("failed to store subscription: %v", err)
	}
	s.AddSubscription(filter, storage.SubscribeOptions{})

	if err := s.Connect(&testConn{}); err != nil {
		t.Fatalf("failed to connect session: %v", err)
	}

	if !connected {
		msg := &storage.Message{
			Topic:    filter,
			Payload:  []byte("hello"),
			QoS:      1,
			PacketID: 7,
		}
		if err := s.Inflight().Add(7, msg, messages.Outbound); err != nil {
			t.Fatalf("failed to add inflight message: %v", err)
		}
		if err := s.OfflineQueue().Enqueue(&storage.Message{
			Topic:   filter,
			Payload: []byte("queued"),
			QoS:     1,
		}); err != nil {
			t.Fatalf("failed to enqueue offline message: %v", err)
		}
		if err := s.Disconnect(false); err != nil {
			t.Fatalf("failed to disconnect session: %v", err)
		}
	}
}
