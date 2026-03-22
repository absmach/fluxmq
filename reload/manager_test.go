// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package reload

import (
	"context"
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
		{Path: "Log.Level", NewValue: "warn"},
		{Path: "Log.Format", NewValue: "json"},
	}

	updated, err := applyFieldChanges(base, changes)
	if err != nil {
		t.Fatal(err)
	}

	if updated.Log.Level != "warn" { //nolint:goconst // test value
		t.Errorf("expected 'warn', got %q", updated.Log.Level)
	}
	if updated.Log.Format != "json" {
		t.Errorf("expected 'json', got %q", updated.Log.Format)
	}

	// Base must not be mutated.
	if base.Log.Level == "warn" {
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
