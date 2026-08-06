// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	corebroker "github.com/absmach/fluxmq/broker"
	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/queue"
	qstorage "github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage/memory"
)

type certificateMetricsStub struct{}

func (certificateMetricsStub) CertificateMetrics() corebroker.CertificateMetrics {
	return corebroker.CertificateMetrics{
		ResolverRequests:   7,
		CacheEntries:       2,
		CacheInvalidations: 3,
	}
}

func TestStatsMQTTOnly(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	b.Stats().IncrementConnections()
	b.Stats().IncrementConnections()
	b.Stats().DecrementConnections()
	b.Stats().IncrementPublishReceived()
	b.Stats().AddBytesReceived(1024)
	b.Stats().IncrementProtocolErrors()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.UptimeSeconds <= 0 {
		t.Fatal("expected positive uptime")
	}
	if resp.Connections.Current != 1 {
		t.Fatalf("expected current connections 1, got %d", resp.Connections.Current)
	}
	if resp.Connections.Total != 2 {
		t.Fatalf("expected total connections 2, got %d", resp.Connections.Total)
	}
	if resp.Messages.Received != 1 {
		t.Fatalf("expected messages received 1, got %d", resp.Messages.Received)
	}
	if resp.Bytes.Received != 1024 {
		t.Fatalf("expected bytes received 1024, got %d", resp.Bytes.Received)
	}
	if resp.Errors.Protocol != 1 {
		t.Fatalf("expected protocol errors 1, got %d", resp.Errors.Protocol)
	}

	// Verify per-protocol breakdown
	if resp.ByProtocol.MQTT == nil {
		t.Fatal("expected mqtt in by_protocol")
	}
	if resp.ByProtocol.MQTT.Messages.PublishReceived != 1 {
		t.Fatalf("expected mqtt publish_received 1, got %d", resp.ByProtocol.MQTT.Messages.PublishReceived)
	}
	if resp.ByProtocol.MQTT.Errors.Auth != 0 {
		t.Fatalf("expected mqtt auth errors 0, got %d", resp.ByProtocol.MQTT.Errors.Auth)
	}
	if resp.ByProtocol.AMQP != nil {
		t.Fatal("expected no amqp in by_protocol when amqp broker is nil")
	}
}

func TestStatsIncludesLabelFreeCertificateMetrics(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())
	srv.SetCertificateMetricsProvider(certificateMetricsStub{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	var response statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.Certificates == nil {
		t.Fatal("expected certificate metrics")
	}
	if response.Certificates.ResolverRequests != 7 || response.Certificates.CacheInvalidations != 3 {
		t.Fatalf("unexpected certificate metrics: %+v", response.Certificates)
	}
}

func TestStatsAMQPOnly(t *testing.T) {
	ab := amqpbroker.New(nil, slog.Default())
	srv := New(Config{}, nil, ab, nil, nil, nil, nil, slog.Default())

	ab.GetStats().IncrementConnections()
	ab.GetStats().IncrementMessagesReceived()
	ab.GetStats().IncrementChannels()
	ab.GetStats().IncrementConsumers()
	ab.GetStats().IncrementLocalConnections()
	ab.GetStats().IncrementLocalAuthSuccess()
	ab.GetStats().IncrementLocalAuthFailures()
	ab.GetStats().IncrementLocalPublishDenials()
	ab.GetStats().IncrementLocalSubscribeDenials()
	ab.GetStats().IncrementLocalOperationDenials()
	ab.GetStats().IncrementLocalReloadSuccess()
	ab.GetStats().IncrementLocalReloadFailures()
	ab.GetStats().AddLocalForcedDisconnects(2)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Connections.Current != 1 {
		t.Fatalf("expected current connections 1, got %d", resp.Connections.Current)
	}
	if resp.Messages.Received != 1 {
		t.Fatalf("expected messages received 1, got %d", resp.Messages.Received)
	}
	if resp.ByProtocol.MQTT != nil {
		t.Fatal("expected no mqtt in by_protocol when mqtt broker is nil")
	}
	if resp.ByProtocol.AMQP == nil {
		t.Fatal("expected amqp in by_protocol")
	}
	if resp.ByProtocol.AMQP.Channels != 1 {
		t.Fatalf("expected amqp channels 1, got %d", resp.ByProtocol.AMQP.Channels)
	}
	if resp.ByProtocol.AMQP.Consumers != 1 {
		t.Fatalf("expected amqp consumers 1, got %d", resp.ByProtocol.AMQP.Consumers)
	}
	local := resp.ByProtocol.AMQP.LocalPrincipals
	if local.ActiveConnections != 1 {
		t.Fatalf("expected one active local-principal connection, got %d", local.ActiveConnections)
	}
	if local.Authentication.Success != 1 || local.Authentication.Failure != 1 {
		t.Fatalf("unexpected local authentication stats: %+v", local.Authentication)
	}
	if local.Authorization.PublishDenied != 1 || local.Authorization.SubscribeDenied != 1 || local.Authorization.OperationDenied != 1 {
		t.Fatalf("unexpected local authorization stats: %+v", local.Authorization)
	}
	if local.Reloads.Success != 1 || local.Reloads.Failure != 1 || local.Reloads.ForcedDisconnects != 2 {
		t.Fatalf("unexpected local reload stats: %+v", local.Reloads)
	}
}

func TestStatsAggregatesBothProtocols(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	ab := amqpbroker.New(nil, slog.Default())
	srv := New(Config{}, b, ab, nil, nil, nil, nil, slog.Default())

	b.Stats().IncrementConnections()
	b.Stats().IncrementPublishReceived()
	b.Stats().AddBytesReceived(100)

	ab.GetStats().IncrementConnections()
	ab.GetStats().IncrementConnections()
	ab.GetStats().IncrementMessagesReceived()
	ab.GetStats().AddBytesReceived(200)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Aggregated totals: 1 MQTT + 2 AMQP = 3 connections
	if resp.Connections.Current != 3 {
		t.Fatalf("expected aggregate current connections 3, got %d", resp.Connections.Current)
	}
	// Messages: 1 MQTT (publish counts as message) + 1 AMQP = 2
	if resp.Messages.Received != 2 {
		t.Fatalf("expected aggregate messages received 2, got %d", resp.Messages.Received)
	}
	// Bytes: 100 + 200 = 300
	if resp.Bytes.Received != 300 {
		t.Fatalf("expected aggregate bytes received 300, got %d", resp.Bytes.Received)
	}

	// Both protocol sections present
	if resp.ByProtocol.MQTT == nil {
		t.Fatal("expected mqtt in by_protocol")
	}
	if resp.ByProtocol.AMQP == nil {
		t.Fatal("expected amqp in by_protocol")
	}
	if resp.ByProtocol.MQTT.Connections.Current != 1 {
		t.Fatalf("expected mqtt connections 1, got %d", resp.ByProtocol.MQTT.Connections.Current)
	}
	if resp.ByProtocol.AMQP.Connections.Current != 2 {
		t.Fatalf("expected amqp connections 2, got %d", resp.ByProtocol.AMQP.Connections.Current)
	}
}

func TestStatsNilBrokersReturns503(t *testing.T) {
	srv := New(Config{}, nil, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestStatsRejectsPost(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// unreadableQueueStore matches a queue in the topic index but cannot return its
// configuration, so the capture is lost during resolution — before any append is
// dispatched, and therefore counted on the publishing goroutine.
type unreadableQueueStore struct {
	qstorage.QueueStore
}

func (s *unreadableQueueStore) GetQueue(_ context.Context, _ string) (*qtypes.QueueConfig, error) {
	return nil, errors.New("configuration unreadable")
}

// A queue silently dropping the traffic its topic pattern binds is the failure
// mode capture introduces, and the counter is its only signal, so it has to
// reach the operator plane rather than stopping at GetMetrics.
func TestStatsReportsQueueMetrics(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	logStore := &unreadableQueueStore{QueueStore: memlog.New()}
	manager := queue.NewManager(logStore, nil, nil, queue.DefaultConfig(), slog.Default(), nil)
	srv := New(Config{}, b, nil, nil, manager, nil, nil, slog.Default())

	ctx := context.Background()
	if err := manager.CreateQueue(ctx, qtypes.DefaultQueueConfig("messages", "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	// Resolution runs on the publishing goroutine, so this loss is counted
	// before anything is dispatched and the endpoint can be read straight after.
	if err := manager.PublishToMatchingQueues(ctx, qtypes.PublishRequest{
		Topic:   "m/acme/temp",
		Payload: []byte("payload"),
	}); err != nil {
		t.Fatalf("capture must not fail the publish: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Queues == nil {
		t.Fatal("expected a queues section when a queue manager is configured")
	}
	if resp.Queues.CaptureFailures != 1 {
		t.Fatalf("capture_failures = %d, want 1", resp.Queues.CaptureFailures)
	}
}

func TestStatsOmitsQueuesWithoutManager(t *testing.T) {
	store := memory.New()
	b := mqttbroker.NewBroker(store, nil, mqttbroker.WithLogger(slog.Default()))
	srv := New(Config{}, b, nil, nil, nil, nil, nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := body["queues"]; present {
		t.Fatal("queues must be omitted when no queue manager is configured")
	}
}
