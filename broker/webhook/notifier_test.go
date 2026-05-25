// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/config"
	"github.com/stretchr/testify/require"
)

// mockSender implements Sender interface for testing.
type mockSender struct {
	mu          sync.Mutex
	sendCount   int32
	sendFunc    func(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error
	lastURL     string
	lastHeaders map[string]string
	lastPayload []byte
}

func newMockSender() *mockSender {
	return &mockSender{
		sendFunc: func(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error {
			return nil // Success by default
		},
	}
}

func (m *mockSender) Send(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error {
	atomic.AddInt32(&m.sendCount, 1)
	m.mu.Lock()
	m.lastURL = url
	m.lastHeaders = headers
	m.lastPayload = payload
	m.mu.Unlock()
	return m.sendFunc(ctx, url, headers, payload, timeout)
}

func (m *mockSender) getSendCount() int {
	return int(atomic.LoadInt32(&m.sendCount))
}

func (m *mockSender) resetCount() {
	atomic.StoreInt32(&m.sendCount, 0)
}

func TestNewNotifier(t *testing.T) {
	cfg := config.WebhookConfig{
		QueueSize:  1000,
		DropPolicy: "oldest",
		Workers:    2,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts:     3,
				InitialInterval: 1 * time.Second,
				MaxInterval:     30 * time.Second,
				Multiplier:      2.0,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 5,
				ResetTimeout:     60 * time.Second,
			},
		},
		Endpoints: []config.WebhookEndpoint{
			{
				Name: "test-endpoint",
				Type: "http",
				URL:  "http://example.com/webhook",
				Headers: map[string]string{
					"Authorization": "Bearer token",
				},
			},
		},
	}

	sender := newMockSender()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}
	defer notifier.Close()

	if notifier == nil {
		t.Fatal("expected notifier, got nil")
	}
	if len(notifier.endpoints) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(notifier.endpoints))
	}
}

func TestNewNotifier_NilSender(t *testing.T) {
	cfg := config.WebhookConfig{
		QueueSize: 100,
		Workers:   1,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
		},
	}

	_, err := NewNotifier(cfg, "broker-1", nil, nil)
	if err == nil {
		t.Error("expected error for nil sender, got nil")
	}
}

func TestNotifier_Notify_Success(t *testing.T) {
	sender := newMockSender()
	cfg := config.WebhookConfig{
		QueueSize:  100,
		DropPolicy: "oldest",
		Workers:    2,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts:     1,
				InitialInterval: 1 * time.Second,
				MaxInterval:     10 * time.Second,
				Multiplier:      2.0,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 5,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 5 * time.Second,
		Endpoints: []config.WebhookEndpoint{
			{
				Name: "test",
				Type: "http",
				URL:  "http://example.com/webhook",
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}
	defer notifier.Close()

	// Send event
	event := events.ClientConnected{
		ClientID:   "client-1",
		Protocol:   "mqtt5",
		CleanStart: true,
		KeepAlive:  60,
		RemoteAddr: "192.168.1.100:1234",
	}

	err = notifier.Notify(context.Background(), event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	if sender.getSendCount() != 1 {
		t.Errorf("expected 1 send, got %d", sender.getSendCount())
	}
}

func TestNotifier_Notify_EventTypeFilter(t *testing.T) {
	sender := newMockSender()
	cfg := config.WebhookConfig{
		QueueSize:  100,
		DropPolicy: "oldest",
		Workers:    1,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts: 1,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 5,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 5 * time.Second,
		Endpoints: []config.WebhookEndpoint{
			{
				Name: "test",
				Type: "http",
				URL:  "http://example.com/webhook",
				Events: []string{
					events.TypeClientConnected,
					events.TypeMessagePublished,
				},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}
	defer notifier.Close()

	// Send matching event
	event1 := events.ClientConnected{ClientID: "client-1"}
	notifier.Notify(context.Background(), event1) //nolint:errcheck // best-effort fire-and-forget in test

	// Send non-matching event
	event2 := events.ClientDisconnected{ClientID: "client-1"}
	notifier.Notify(context.Background(), event2) //nolint:errcheck // best-effort fire-and-forget in test

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Only matching event should be sent
	if sender.getSendCount() != 1 {
		t.Errorf("expected 1 send (filtered), got %d", sender.getSendCount())
	}
}

func TestNotifier_Notify_TopicFilter(t *testing.T) {
	sender := newMockSender()
	cfg := config.WebhookConfig{
		QueueSize:  100,
		DropPolicy: "oldest",
		Workers:    1,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts: 1,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 5,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 5 * time.Second,
		Endpoints: []config.WebhookEndpoint{
			{
				Name: "test",
				Type: "http",
				URL:  "http://example.com/webhook",
				TopicFilters: []string{
					"sensors/#",
					"devices/+/telemetry",
				},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}
	defer notifier.Close()

	tests := []struct {
		topic       string
		shouldMatch bool
	}{
		{"sensors/temperature", true},
		{"sensors/humidity/room1", true},
		{"devices/device1/telemetry", true},
		{"devices/device2/telemetry", true},
		{"other/topic", false},
		{"devices/device1/status", false},
	}

	for _, tt := range tests {
		sender.resetCount()

		event := events.MessagePublished{
			MessageTopic: tt.topic,
			ClientID:     "client-1",
			QoS:          1,
		}
		notifier.Notify(context.Background(), event) //nolint:errcheck // best-effort fire-and-forget in test
		time.Sleep(50 * time.Millisecond)

		expected := 0
		if tt.shouldMatch {
			expected = 1
		}
		if sender.getSendCount() != expected {
			t.Errorf("topic %s: expected %d sends, got %d", tt.topic, expected, sender.getSendCount())
		}
	}
}

func TestNotifier_Retry(t *testing.T) {
	attemptCount := int32(0)
	sender := newMockSender()
	sender.sendFunc = func(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error {
		count := atomic.AddInt32(&attemptCount, 1)
		if count < 3 {
			return errors.New("temporary failure")
		}
		return nil // Success on 3rd attempt
	}

	cfg := config.WebhookConfig{
		QueueSize:  100,
		DropPolicy: "oldest",
		Workers:    1,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts:     3,
				InitialInterval: 50 * time.Millisecond,
				MaxInterval:     1 * time.Second,
				Multiplier:      2.0,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 10,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 5 * time.Second,
		Endpoints: []config.WebhookEndpoint{
			{
				Name: "test",
				Type: "http",
				URL:  "http://example.com/webhook",
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}
	defer notifier.Close()

	event := events.ClientConnected{ClientID: "client-1"}
	notifier.Notify(context.Background(), event) //nolint:errcheck // best-effort fire-and-forget in test

	// Wait for retries
	time.Sleep(500 * time.Millisecond)

	if atomic.LoadInt32(&attemptCount) != 3 {
		t.Errorf("expected 3 attempts (2 retries), got %d", attemptCount)
	}
}

func TestNotifier_QueueOverflow_DropOldest(t *testing.T) {
	sender := newMockSender()
	// Make sender slow
	sender.sendFunc = func(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	cfg := config.WebhookConfig{
		QueueSize:  5, // Small queue
		DropPolicy: "oldest",
		Workers:    1,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts: 1,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 10,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 5 * time.Second,
		Endpoints: []config.WebhookEndpoint{
			{Name: "test", Type: "http", URL: "http://example.com/webhook"},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}
	defer notifier.Close()

	// Send more events than queue can hold
	for i := 0; i < 10; i++ {
		event := events.ClientConnected{ClientID: "client-1"}
		notifier.Notify(context.Background(), event) //nolint:errcheck // best-effort fire-and-forget in test
	}

	// Wait for processing
	time.Sleep(1500 * time.Millisecond)

	// With drop oldest policy and slow processing, some events should be dropped
	// but we should have processed more than the queue size
	count := sender.getSendCount()
	if count == 0 {
		t.Error("expected some events to be processed")
	}
	t.Logf("processed %d events with queue size 5", count)
}

func TestTopicMatches(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		match  bool
	}{
		// Exact matches
		{"sensors/temp", "sensors/temp", true},
		{"sensors/temp", "sensors/humidity", false},

		// Single-level wildcard (+)
		{"sensors/+", "sensors/temp", true},
		{"sensors/+", "sensors/humidity", true},
		{"sensors/+", "sensors", false},
		{"sensors/+", "sensors/temp/room1", false},
		{"sensors/+/temp", "sensors/device1/temp", true},
		{"sensors/+/temp", "sensors/device1/humidity", false},

		// Multi-level wildcard (#)
		{"sensors/#", "sensors/temp", true},
		{"sensors/#", "sensors/temp/room1", true},
		{"sensors/#", "sensors", true},
		{"sensors/#", "devices/temp", false},
		{"sensors/temp/#", "sensors/temp/room1/device1", true},

		// Mixed
		{"sensors/+/temp/#", "sensors/device1/temp/room1", true},
		{"sensors/+/temp/#", "sensors/device1/humidity/room1", false},
	}

	for _, tt := range tests {
		result := topicMatches(tt.filter, tt.topic)
		if result != tt.match {
			t.Errorf("topicMatches(%q, %q) = %v, want %v", tt.filter, tt.topic, result, tt.match)
		}
	}
}

func baseWebhookCfg(workers, queueSize int) config.WebhookConfig {
	return config.WebhookConfig{
		Enabled:    true,
		Workers:    workers,
		QueueSize:  queueSize,
		DropPolicy: "oldest",
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry:   config.RetryConfig{MaxAttempts: 1},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 10,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 500 * time.Millisecond,
		Endpoints:       []config.WebhookEndpoint{{Name: "ep", Type: "http", URL: "http://example.com/hook"}},
	}
}

func TestAtomicNotifier_NotifyWhenDisabled(t *testing.T) {
	cfg := baseWebhookCfg(2, 100)
	cfg.Enabled = false
	cfg.Endpoints = nil

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	an, err := NewAtomicNotifier(cfg, "broker-1", newMockSender(), logger)
	if err != nil {
		t.Fatal(err)
	}
	defer an.Close()

	if err := an.Notify(context.Background(), events.ClientConnected{ClientID: "c1"}); err != nil {
		t.Errorf("Notify on disabled AtomicNotifier should return nil, got %v", err)
	}
}

func TestAtomicNotifier_ReconfigureEnablesNotifier(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	sender := newMockSender()

	// Start disabled.
	disabled := baseWebhookCfg(2, 100)
	disabled.Enabled = false
	disabled.Endpoints = nil

	an, err := NewAtomicNotifier(disabled, "broker-1", sender, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer an.Close()

	// Notify is a no-op while disabled.
	if err := an.Notify(context.Background(), events.ClientConnected{ClientID: "c1"}); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if sender.getSendCount() != 0 {
		t.Errorf("expected 0 sends while disabled, got %d", sender.getSendCount())
	}

	// Enable via Reconfigure.
	enabled := baseWebhookCfg(1, 100)
	if err := an.Reconfigure(enabled); err != nil {
		t.Fatalf("Reconfigure failed: %v", err)
	}

	an.Notify(context.Background(), events.ClientConnected{ClientID: "c1"}) //nolint:errcheck

	// Give the worker time to process.
	require.Eventually(t, func() bool {
		return sender.getSendCount() > 0
	}, time.Second, 10*time.Millisecond, "expected at least one send after enabling")
}

func TestAtomicNotifier_ReconfigureDisablesNotifier(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	sender := newMockSender()

	cfg := baseWebhookCfg(2, 100)
	an, err := NewAtomicNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Disable via Reconfigure.
	disabled := cfg
	disabled.Enabled = false
	disabled.Endpoints = nil
	if err := an.Reconfigure(disabled); err != nil {
		t.Fatalf("Reconfigure failed: %v", err)
	}
	defer an.Close()

	sender.resetCount()
	an.Notify(context.Background(), events.ClientConnected{ClientID: "c1"}) //nolint:errcheck
	time.Sleep(50 * time.Millisecond)
	if sender.getSendCount() != 0 {
		t.Errorf("expected 0 sends after disabling, got %d", sender.getSendCount())
	}
}

func TestAtomicNotifier_ReconfigureSwapsWorkerCount(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	sender := newMockSender()

	cfg := baseWebhookCfg(1, 100)
	an, err := NewAtomicNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer an.Close()

	// Reconfigure with more workers — should succeed without blocking.
	newCfg := baseWebhookCfg(4, 200)
	if err := an.Reconfigure(newCfg); err != nil {
		t.Fatalf("Reconfigure failed: %v", err)
	}

	// Verify it still delivers after reconfigure.
	an.Notify(context.Background(), events.ClientConnected{ClientID: "c1"}) //nolint:errcheck
	require.Eventually(t, func() bool {
		return sender.getSendCount() > 0
	}, time.Second, 10*time.Millisecond, "expected delivery after reconfigure")
}

func TestAtomicNotifier_CloseIdempotent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := baseWebhookCfg(1, 10)

	an, err := NewAtomicNotifier(cfg, "broker-1", newMockSender(), logger)
	if err != nil {
		t.Fatal(err)
	}

	if err := an.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := an.Close(); err != nil {
		t.Fatalf("second Close should be idempotent, got: %v", err)
	}
}

func TestNotifier_GracefulShutdown(t *testing.T) {
	processedCount := int32(0)
	sender := newMockSender()
	sender.sendFunc = func(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error {
		atomic.AddInt32(&processedCount, 1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	cfg := config.WebhookConfig{
		QueueSize:  100,
		DropPolicy: "oldest",
		Workers:    3,
		Defaults: config.WebhookDefaults{
			Timeout: 5 * time.Second,
			Retry: config.RetryConfig{
				MaxAttempts: 1,
			},
			CircuitBreaker: config.CircuitBreakerConfig{
				FailureThreshold: 10,
				ResetTimeout:     10 * time.Second,
			},
		},
		ShutdownTimeout: 1 * time.Second,
		Endpoints: []config.WebhookEndpoint{
			{Name: "test", Type: "http", URL: "http://example.com/webhook"},
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	notifier, err := NewNotifier(cfg, "broker-1", sender, logger)
	if err != nil {
		t.Fatalf("failed to create notifier: %v", err)
	}

	// Send some events
	for i := 0; i < 5; i++ {
		event := events.ClientConnected{ClientID: "client-1"}
		notifier.Notify(context.Background(), event) //nolint:errcheck // best-effort fire-and-forget in test
	}

	// Give workers a moment to start processing
	time.Sleep(50 * time.Millisecond)

	// Close should wait for in-progress events to complete
	notifier.Close()

	// Verify some events were processed during graceful shutdown
	processed := atomic.LoadInt32(&processedCount)
	if processed == 0 {
		t.Error("expected at least some events to be processed during graceful shutdown, got 0")
	}
	t.Logf("processed %d events during graceful shutdown", processed)
}
