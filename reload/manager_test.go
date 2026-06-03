// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package reload

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/ratelimit"
)

// mockBrokerTuner records SetMaxQoS/MaxQoS calls for testing.
type mockBrokerTuner struct {
	maxQoS atomic.Uint32
}

func newMockBrokerTuner(qos byte) *mockBrokerTuner {
	b := &mockBrokerTuner{}
	b.maxQoS.Store(uint32(qos))
	return b
}

func (b *mockBrokerTuner) SetMaxQoS(qos byte) { b.maxQoS.Store(uint32(qos)) }
func (b *mockBrokerTuner) MaxQoS() byte       { return byte(b.maxQoS.Load()) }

func writeConfig(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReloadNoChanges(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	// Write the default config as YAML (use an empty file — Load returns defaults).
	path := writeConfig(t, dir, "")

	m := New(path, cfg)
	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.HasChanges() {
		t.Error("expected no changes for identical config")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1, got %d", m.Version())
	}
}

func (r *ReloadResult) HasChanges() bool {
	return len(r.Applied) > 0 || len(r.RestartRequired) > 0
}

func TestReloadLogLevel(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `log:
  level: debug
  format: json
`
	path := writeConfig(t, dir, yamlContent)

	var appliedLog config.LogConfig
	m := New(path, cfg,
		WithLogSetup(func(cfg config.LogConfig) {
			appliedLog = cfg
		}),
	)

	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if appliedLog.Level != "debug" { //nolint:goconst // test value
		t.Errorf("expected log level 'debug', got %q", appliedLog.Level)
	}
	if appliedLog.Format != "json" { //nolint:goconst // test value
		t.Errorf("expected log format 'json', got %q", appliedLog.Format)
	}
	if len(result.Applied) == 0 {
		t.Error("expected at least one applied change")
	}
	if m.Version() != 2 {
		t.Errorf("version should be 2, got %d", m.Version())
	}
}

func TestReloadRateLimits(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `ratelimit:
  enabled: true
  message:
    enabled: true
    rate: 500
    burst: 50
`
	path := writeConfig(t, dir, yamlContent)

	rlManager := ratelimit.NewManager(ratelimit.Config{Enabled: false})
	atomicRL := ratelimit.NewAtomicManager(rlManager)

	m := New(path, cfg, WithRateLimiter(atomicRL))

	_, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// After reload, the rate limiter should be swapped.
	// The new one should enforce rate limits (enabled: true).
	newRL := atomicRL.Load()
	if newRL == rlManager {
		t.Error("rate limiter should have been swapped")
	}
}

func TestReloadBrokerMaxQoS(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `broker:
  max_qos: 1
`
	path := writeConfig(t, dir, yamlContent)

	broker := newMockBrokerTuner(2)
	m := New(path, cfg, WithBroker(broker))

	_, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if broker.MaxQoS() != 1 {
		t.Errorf("expected MaxQoS 1, got %d", broker.MaxQoS())
	}
}

func TestReloadRestartRequired(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `server:
  tcp:
    v3:
      addr: ":9999"
`
	path := writeConfig(t, dir, yamlContent)

	m := New(path, cfg)
	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RestartRequired) == 0 {
		t.Error("expected restart-required changes for listener addr change")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1 when no runtime changes are applied, got %d", m.Version())
	}
	current := m.Current()
	if current.Server.TCP.V3.Addr != cfg.Server.TCP.V3.Addr {
		t.Errorf("runtime snapshot drifted; expected %q, got %q", cfg.Server.TCP.V3.Addr, current.Server.TCP.V3.Addr)
	}
}

func TestReloadRestartRequiredStaysPending(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `server:
  tcp:
    v3:
      addr: ":9999"
`
	path := writeConfig(t, dir, yamlContent)

	m := New(path, cfg)
	first, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.RestartRequired) == 0 {
		t.Fatal("expected restart-required change on first reload")
	}

	second, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(second.RestartRequired) == 0 {
		t.Error("expected restart-required change to remain pending on subsequent reload")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1 while only restart-required changes exist, got %d", m.Version())
	}
}

func TestReloadInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	// Write invalid YAML.
	path := writeConfig(t, dir, `broker:
  max_message_size: 100
`)
	m := New(path, cfg)
	_, err := m.Reload(context.Background())
	if err == nil {
		t.Error("expected error for invalid config (max_message_size too small)")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1 after failed reload, got %d", m.Version())
	}
}

func TestReloadMissingFile(t *testing.T) {
	cfg := config.Default()
	m := New("/nonexistent/path/config.yaml", cfg)

	// config.Load returns defaults for missing files, so this should succeed
	// but produce no changes (since defaults == defaults).
	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.HasChanges() {
		t.Error("expected no changes when loading defaults from missing file")
	}
}

func TestReloadShuttingDown(t *testing.T) {
	cfg := config.Default()
	m := New("", cfg)
	m.Shutdown()

	_, err := m.Reload(context.Background())
	if err == nil {
		t.Error("expected error when reloading during shutdown")
	}
}

func TestReloadConcurrentSerialization(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `log:
  level: debug
`
	path := writeConfig(t, dir, yamlContent)

	var calls int
	m := New(path, cfg,
		WithLogSetup(func(_ config.LogConfig) {
			calls++
		}),
	)

	var wg sync.WaitGroup
	const goroutines = 10
	errs := make(chan error, goroutines)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Reload(context.Background())
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}

	// Only the first reload should apply log changes (subsequent ones see no diff).
	if calls != 1 {
		t.Errorf("expected logSetup called once (first reload applies, rest see no diff), got %d", calls)
	}
}

// mockWebhookTuner records Reconfigure calls for testing.
type mockWebhookTuner struct {
	mu   sync.Mutex
	cfgs []config.WebhookConfig
	err  error
}

func (m *mockWebhookTuner) Reconfigure(cfg config.WebhookConfig) error {
	m.mu.Lock()
	m.cfgs = append(m.cfgs, cfg)
	m.mu.Unlock()
	return m.err
}

func (m *mockWebhookTuner) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.cfgs)
}

func (m *mockWebhookTuner) last() config.WebhookConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfgs[len(m.cfgs)-1]
}

func TestReloadWebhookWorkers(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `webhook:
  enabled: true
  workers: 8
  queue_size: 200
`
	path := writeConfig(t, dir, yamlContent)

	tuner := &mockWebhookTuner{}
	m := New(path, cfg, WithWebhookTuner(tuner))

	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Applied) == 0 {
		t.Error("expected applied webhook changes")
	}
	if len(result.RestartRequired) != 0 {
		t.Errorf("expected no restart-required changes for webhook, got %v", result.RestartRequired)
	}
	if tuner.calls() != 1 {
		t.Errorf("expected 1 Reconfigure call, got %d", tuner.calls())
	}
	if tuner.last().Workers != 8 {
		t.Errorf("expected workers=8, got %d", tuner.last().Workers)
	}
	if m.Version() != 2 {
		t.Errorf("expected version 2, got %d", m.Version())
	}
}

func TestReloadWebhookRollbackOnError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `webhook:
  enabled: true
  workers: 8
`
	path := writeConfig(t, dir, yamlContent)

	tuner := &mockWebhookTuner{err: errors.New("reconfigure failed")}
	m := New(path, cfg, WithWebhookTuner(tuner))

	_, err := m.Reload(context.Background())
	if err == nil {
		t.Error("expected error when WebhookTuner.Reconfigure fails")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1 after failed reload, got %d", m.Version())
	}
	// Only the failing apply call is made; no prior successful subsystem needs rollback.
	if tuner.calls() != 1 {
		t.Errorf("expected 1 Reconfigure call, got %d", tuner.calls())
	}
}

func TestReloadWebhookRollbackRevertsEarlierSubsystems(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	// Both log and webhook change; webhook fails → log should roll back.
	yamlContent := `log:
  level: debug
webhook:
  enabled: true
  workers: 8
`
	path := writeConfig(t, dir, yamlContent)

	var logApplies []string
	tuner := &mockWebhookTuner{err: errors.New("reconfigure failed")}

	m := New(path, cfg,
		WithLogSetup(func(c config.LogConfig) { logApplies = append(logApplies, c.Level) }),
		WithWebhookTuner(tuner),
	)

	_, err := m.Reload(context.Background())
	if err == nil {
		t.Error("expected error when WebhookTuner.Reconfigure fails")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1 after failed reload, got %d", m.Version())
	}
	// logSetup called twice: once to apply "debug", once to rollback to original.
	if len(logApplies) != 2 {
		t.Errorf("expected 2 logSetup calls (apply + rollback), got %d", len(logApplies))
	}
	if logApplies[1] == "debug" {
		t.Error("rollback should have reverted log level from 'debug'")
	}
}

func TestSetFieldByPath(t *testing.T) {
	t.Run("set leaf value", func(t *testing.T) {
		cfg := config.Default()
		if err := setFieldByPath(cfg, "Log.Level", "debug"); err != nil {
			t.Fatal(err)
		}
		if cfg.Log.Level != "debug" {
			t.Errorf("expected 'debug', got %q", cfg.Log.Level)
		}
	})

	t.Run("set nested value", func(t *testing.T) {
		cfg := config.Default()
		if err := setFieldByPath(cfg, "RateLimit.Message.Rate", float64(999)); err != nil {
			t.Fatal(err)
		}
		if cfg.RateLimit.Message.Rate != 999 {
			t.Errorf("expected 999, got %v", cfg.RateLimit.Message.Rate)
		}
	})

	t.Run("invalid path", func(t *testing.T) {
		cfg := config.Default()
		if err := setFieldByPath(cfg, "No.Such.Field", "x"); err == nil {
			t.Error("expected error for invalid path")
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		cfg := config.Default()
		// struct{} is neither assignable nor convertible to string.
		if err := setFieldByPath(cfg, "Log.Level", struct{}{}); err == nil {
			t.Error("expected error for type mismatch (struct{} to string)")
		}
	})
}

func TestApplyFieldChanges(t *testing.T) {
	base := config.Default()
	changes := []config.FieldChange{
		{Path: "Log.Level", NewValue: logLevelWarn},
		{Path: "Log.Format", NewValue: logFormatJSON},
	}

	updated, err := applyFieldChanges(base, changes)
	if err != nil {
		t.Fatal(err)
	}

	if updated.Log.Level != logLevelWarn {
		t.Errorf("expected 'warn', got %q", updated.Log.Level)
	}
	if updated.Log.Format != logFormatJSON {
		t.Errorf("expected 'json', got %q", updated.Log.Format)
	}

	// Base must not be mutated.
	if base.Log.Level == logLevelWarn {
		t.Error("applyFieldChanges mutated the base config")
	}
}

func TestReloadMixedChanges(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `log:
  level: debug
server:
  tcp:
    v3:
      addr: ":9999"
`
	path := writeConfig(t, dir, yamlContent)

	var appliedLog config.LogConfig
	m := New(path, cfg,
		WithLogSetup(func(cfg config.LogConfig) {
			appliedLog = cfg
		}),
	)

	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if appliedLog.Level != "debug" {
		t.Errorf("expected log level 'debug', got %q", appliedLog.Level)
	}
	if len(result.Applied) == 0 {
		t.Error("expected applied changes")
	}
	if len(result.RestartRequired) == 0 {
		t.Error("expected restart-required changes")
	}
	if m.Version() != 2 {
		t.Errorf("version should be 2 after applying runtime-safe changes, got %d", m.Version())
	}

	// On the next reload with unchanged file, runtime-safe changes should be a no-op
	// while restart-required changes remain pending.
	next, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Applied) != 0 {
		t.Errorf("expected no additional runtime-safe applies, got %d", len(next.Applied))
	}
	if len(next.RestartRequired) == 0 {
		t.Error("expected restart-required changes to remain pending")
	}
	if m.Version() != 2 {
		t.Errorf("version should remain 2 when no new runtime-safe changes are applied, got %d", m.Version())
	}
}

// mockSessionTuner records SetSessionConfig calls for testing.
type mockSessionTuner struct {
	mu   sync.Mutex
	cfgs []config.SessionConfig
}

func (m *mockSessionTuner) SetSessionConfig(cfg config.SessionConfig) {
	m.mu.Lock()
	m.cfgs = append(m.cfgs, cfg)
	m.mu.Unlock()
}

func (m *mockSessionTuner) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.cfgs)
}

func (m *mockSessionTuner) last() config.SessionConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfgs[len(m.cfgs)-1]
}

func TestReloadSessionConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	yamlContent := `session:
  max_offline_queue_size: 500
  max_inflight_messages: 64
  offline_queue_policy: evict
`
	path := writeConfig(t, dir, yamlContent)

	tuner := &mockSessionTuner{}
	m := New(path, cfg, WithSessionTuner(tuner))

	result, err := m.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Applied) == 0 {
		t.Error("expected applied session changes")
	}
	if len(result.RestartRequired) != 0 {
		t.Errorf("expected no restart-required changes for session, got %v", result.RestartRequired)
	}
	if tuner.calls() != 1 {
		t.Errorf("expected 1 SetSessionConfig call, got %d", tuner.calls())
	}
	last := tuner.last()
	if last.MaxOfflineQueueSize != 500 {
		t.Errorf("expected MaxOfflineQueueSize=500, got %d", last.MaxOfflineQueueSize)
	}
	if last.MaxInflightMessages != 64 {
		t.Errorf("expected MaxInflightMessages=64, got %d", last.MaxInflightMessages)
	}
	if last.OfflineQueuePolicy != config.OfflineQueuePolicyEvict {
		t.Errorf("expected OfflineQueuePolicy=%q, got %q", config.OfflineQueuePolicyEvict, last.OfflineQueuePolicy)
	}
	if m.Version() != 2 {
		t.Errorf("expected version 2, got %d", m.Version())
	}
}

func TestReloadSessionRollbackRevertsEarlierSubsystems(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	// Log changes first (applies OK), then session changes; webhook fails → everything rolls back.
	// But session has no error path (SetSessionConfig returns nothing), so let's use
	// log + session where session is the last: actually session never errors.
	// Instead test that when broker max_qos (which rolls back) fails, session also rolls back.
	// Easier: log succeeds, session succeeds, webhook fails → both log and session roll back.
	yamlContent := `log:
  level: debug
session:
  max_inflight_messages: 32
webhook:
  enabled: true
  workers: 8
`
	path := writeConfig(t, dir, yamlContent)

	var logApplies []string
	sessionTuner := &mockSessionTuner{}
	webhookTuner := &mockWebhookTuner{err: errors.New("reconfigure failed")}

	m := New(path, cfg,
		WithLogSetup(func(c config.LogConfig) { logApplies = append(logApplies, c.Level) }),
		WithSessionTuner(sessionTuner),
		WithWebhookTuner(webhookTuner),
	)

	_, err := m.Reload(context.Background())
	if err == nil {
		t.Error("expected error when WebhookTuner.Reconfigure fails")
	}
	if m.Version() != 1 {
		t.Errorf("version should remain 1 after failed reload, got %d", m.Version())
	}
	// log: apply "debug" + rollback → 2 calls
	if len(logApplies) != 2 {
		t.Errorf("expected 2 logSetup calls (apply + rollback), got %d", len(logApplies))
	}
	// session: apply (new config) + rollback (old config) → 2 calls
	if sessionTuner.calls() != 2 {
		t.Errorf("expected 2 SetSessionConfig calls (apply + rollback), got %d", sessionTuner.calls())
	}
}
