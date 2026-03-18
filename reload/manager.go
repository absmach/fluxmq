// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package reload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/ratelimit"
)

// BrokerTuner applies broker-level config changes atomically.
type BrokerTuner interface {
	SetMaxQoS(qos byte)
	MaxQoS() byte
}

// ErrShuttingDown indicates reload was rejected because shutdown has started.
var ErrShuttingDown = errors.New("server is shutting down")

// ReloadResult describes the outcome of a reload operation.
type ReloadResult struct {
	Version         uint64               `json:"version"`
	Applied         []config.FieldChange `json:"applied,omitempty"`
	RestartRequired []config.FieldChange `json:"restart_required,omitempty"`
	Errors          []string             `json:"errors,omitempty"`
	Duration        time.Duration        `json:"duration"`
}

// Manager orchestrates hot config reload by re-reading the config file,
// diffing against the current config, and applying runtime-safe changes.
type Manager struct {
	mu           sync.Mutex
	configFile   string
	current      *config.Config
	version      uint64
	shuttingDown atomic.Bool

	// Subsystem references for direct calls (decision #1 from eng review).
	rateLimiter *ratelimit.AtomicManager
	broker      BrokerTuner
	logSetup    func(cfg config.LogConfig)
}

// Option configures the Manager.
type Option func(*Manager)

// WithRateLimiter sets the rate limiter for reload updates.
func WithRateLimiter(rl *ratelimit.AtomicManager) Option {
	return func(m *Manager) { m.rateLimiter = rl }
}

// WithBroker sets the broker for reload updates.
func WithBroker(b BrokerTuner) Option {
	return func(m *Manager) { m.broker = b }
}

// WithLogSetup sets the function called to reconfigure logging.
func WithLogSetup(fn func(cfg config.LogConfig)) Option {
	return func(m *Manager) { m.logSetup = fn }
}

// New creates a new reload Manager.
func New(configFile string, initial *config.Config, opts ...Option) *Manager {
	m := &Manager{
		configFile: configFile,
		current:    initial,
		version:    1,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Shutdown marks the manager as shutting down. Subsequent reload attempts
// are rejected with an error.
func (m *Manager) Shutdown() {
	m.shuttingDown.Store(true)
}

// Version returns the current config version.
func (m *Manager) Version() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version
}

// Current returns a copy of the current config.
func (m *Manager) Current() config.Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *m.current
}

// Reload re-reads the config file, diffs against the current config,
// applies runtime-safe changes, and returns a detailed result.
func (m *Manager) Reload(ctx context.Context) (*ReloadResult, error) {
	if m.shuttingDown.Load() {
		return nil, fmt.Errorf("reload rejected: %w", ErrShuttingDown)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	start := time.Now()
	result := &ReloadResult{Version: m.version}

	newCfg, err := config.Load(m.configFile)
	if err != nil {
		return nil, fmt.Errorf("reload failed: %w", err)
	}

	diff := config.Diff(m.current, newCfg)
	if !diff.HasChanges() {
		result.Duration = time.Since(start)
		slog.Info("config reload: no changes detected", "version", m.version)
		return result, nil
	}

	result.RestartRequired = diff.RestartRequired

	if len(diff.RuntimeSafe) > 0 {
		applied, errs := m.applyRuntimeChanges(ctx, m.current, newCfg, diff.RuntimeSafe)
		result.Applied = applied
		if len(errs) > 0 {
			result.Errors = errs
			result.Duration = time.Since(start)
			return result, fmt.Errorf("reload partially failed: %d errors", len(errs))
		}
	}

	// Update current runtime snapshot using only actually applied changes.
	// Restart-required-only changes must remain pending until process restart.
	if len(result.Applied) > 0 {
		updated, updateErr := applyFieldChanges(m.current, result.Applied)
		if updateErr != nil {
			result.Errors = append(result.Errors, updateErr.Error())
			result.Duration = time.Since(start)
			return result, fmt.Errorf("reload failed to update runtime snapshot: %w", updateErr)
		}
		m.current = updated
		m.version++
	}
	result.Version = m.version
	result.Duration = time.Since(start)

	m.logAudit(result)
	return result, nil
}

func (m *Manager) applyRuntimeChanges(_ context.Context, old, new *config.Config, changes []config.FieldChange) (applied []config.FieldChange, errs []string) {
	var rollbacks []func()

	// applySubsystem applies one subsystem-level change. On failure, rolls back
	// all previously applied subsystems and returns false.
	applySubsystem := func(subsystemChanges []config.FieldChange, applyFn func() error, rollbackFn func()) bool {
		if err := applyFn(); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", subsystemChanges[0].Path, err))
			for i := len(rollbacks) - 1; i >= 0; i-- {
				rollbacks[i]()
			}
			applied = nil
			return false
		}
		applied = append(applied, subsystemChanges...)
		rollbacks = append(rollbacks, rollbackFn)
		return true
	}

	// Partition changes by subsystem.
	var logChanges, rateLimitChanges, brokerChanges []config.FieldChange
	for _, c := range changes {
		switch {
		case isLogField(c.Path):
			logChanges = append(logChanges, c)
		case isRateLimitField(c.Path):
			rateLimitChanges = append(rateLimitChanges, c)
		case isBrokerField(c.Path):
			brokerChanges = append(brokerChanges, c)
		}
	}

	if len(logChanges) > 0 && m.logSetup != nil {
		if !applySubsystem(logChanges, func() error {
			m.logSetup(new.Log)
			return nil
		}, func() {
			m.logSetup(old.Log)
		}) {
			return
		}
	}

	if len(rateLimitChanges) > 0 && m.rateLimiter != nil {
		if !applySubsystem(rateLimitChanges, func() error {
			newManager := ratelimit.NewManager(configToRatelimit(new.RateLimit))
			m.rateLimiter.Swap(newManager)
			return nil
		}, func() {
			oldManager := ratelimit.NewManager(configToRatelimit(old.RateLimit))
			m.rateLimiter.Swap(oldManager)
		}) {
			return
		}
	}

	if len(brokerChanges) > 0 && m.broker != nil {
		oldQoS := m.broker.MaxQoS()
		if !applySubsystem(brokerChanges, func() error {
			m.broker.SetMaxQoS(byte(new.Broker.MaxQoS))
			return nil
		}, func() {
			m.broker.SetMaxQoS(oldQoS)
		}) {
			return
		}
	}

	return
}

func configToRatelimit(cfg config.RateLimitConfig) ratelimit.Config {
	return ratelimit.Config{
		Enabled: cfg.Enabled,
		Connection: ratelimit.ConnectionConfig{
			Enabled:         cfg.Connection.Enabled,
			Rate:            cfg.Connection.Rate,
			Burst:           cfg.Connection.Burst,
			CleanupInterval: cfg.Connection.CleanupInterval,
		},
		Message: ratelimit.MessageConfig{
			Enabled: cfg.Message.Enabled,
			Rate:    cfg.Message.Rate,
			Burst:   cfg.Message.Burst,
		},
		Subscribe: ratelimit.SubscribeConfig{
			Enabled: cfg.Subscribe.Enabled,
			Rate:    cfg.Subscribe.Rate,
			Burst:   cfg.Subscribe.Burst,
		},
	}
}

func (m *Manager) logAudit(result *ReloadResult) {
	attrs := []any{
		slog.Uint64("version", result.Version),
		slog.Int("applied_count", len(result.Applied)),
		slog.Int("restart_required_count", len(result.RestartRequired)),
		slog.Int("error_count", len(result.Errors)),
		slog.Duration("duration", result.Duration),
	}

	appliedPaths := make([]string, len(result.Applied))
	for i, c := range result.Applied {
		appliedPaths[i] = c.Path
	}
	if len(appliedPaths) > 0 {
		attrs = append(attrs, slog.Any("applied_fields", appliedPaths))
	}

	restartPaths := make([]string, len(result.RestartRequired))
	for i, c := range result.RestartRequired {
		restartPaths[i] = c.Path
	}
	if len(restartPaths) > 0 {
		attrs = append(attrs, slog.Any("restart_required_fields", restartPaths))
	}

	if len(result.Errors) > 0 {
		slog.Error("config reload completed with errors", attrs...)
	} else {
		slog.Info("config reload completed", attrs...)
	}
}

func isLogField(path string) bool       { return len(path) > 4 && path[:4] == "Log." }
func isRateLimitField(path string) bool { return len(path) > 10 && path[:10] == "RateLimit." }
func isBrokerField(path string) bool    { return len(path) > 7 && path[:7] == "Broker." }

// applyFieldChanges returns a new Config with only the given field changes
// applied on top of base. The shallow copy is safe because all current
// runtime-safe fields (log, ratelimit, broker tuning) are value types.
// If a slice/map-typed field is ever promoted to runtime-safe, this must
// be updated to deep-copy the relevant section.
func applyFieldChanges(base *config.Config, changes []config.FieldChange) (*config.Config, error) {
	updated := *base

	for _, change := range changes {
		if err := setFieldByPath(&updated, change.Path, change.NewValue); err != nil {
			return nil, err
		}
	}

	return &updated, nil
}

func setFieldByPath(cfg *config.Config, path string, value any) error {
	parts := strings.Split(path, ".")
	current := reflect.ValueOf(cfg).Elem()

	for i, part := range parts {
		field := current.FieldByName(part)
		if !field.IsValid() {
			return fmt.Errorf("unknown config field %q", path)
		}

		if i == len(parts)-1 {
			newVal := reflect.ValueOf(value)
			if !newVal.IsValid() {
				field.Set(reflect.Zero(field.Type()))
				return nil
			}
			if newVal.Type().AssignableTo(field.Type()) {
				field.Set(newVal)
				return nil
			}
			if newVal.Type().ConvertibleTo(field.Type()) {
				field.Set(newVal.Convert(field.Type()))
				return nil
			}
			return fmt.Errorf("cannot assign %s to %s for %q", newVal.Type(), field.Type(), path)
		}

		if field.Kind() == reflect.Pointer {
			if field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			field = field.Elem()
		}
		if field.Kind() != reflect.Struct {
			return fmt.Errorf("field %q in path %q is not a struct", part, path)
		}
		current = field
	}

	return nil
}

// SetupLogger creates a new slog default logger from the given log config.
// This is the default logSetup function.
func SetupLogger(cfg config.LogConfig) {
	logLevel := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	slog.SetDefault(slog.New(handler))
}
