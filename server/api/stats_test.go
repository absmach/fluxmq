// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage/memory"
)

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

func TestStatsAMQPOnly(t *testing.T) {
	ab := amqpbroker.New(nil, slog.Default())
	srv := New(Config{}, nil, ab, nil, nil, nil, nil, slog.Default())

	ab.GetStats().IncrementConnections()
	ab.GetStats().IncrementMessagesReceived()
	ab.GetStats().IncrementChannels()
	ab.GetStats().IncrementConsumers()

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
