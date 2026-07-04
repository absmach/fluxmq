// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"log/slog"
	"time"

	"github.com/sony/gobreaker"
)

const (
	defaultTimeout         = 500 * time.Millisecond
	defaultCBMaxRequests   = 3
	defaultCBOpenInterval  = 10 * time.Second
	defaultCBFailThreshold = 5
)

// Options holds hook callout options.
type Options struct {
	Timeout time.Duration
	Logger  *slog.Logger
	CB      *gobreaker.CircuitBreaker
}

// Option configures a hook callout client.
type Option func(*Options)

// WithTimeout sets the per-call timeout.
func WithTimeout(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.Timeout = d
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(o *Options) {
		if l != nil {
			o.Logger = l
		}
	}
}

// WithCircuitBreaker overrides the default circuit breaker.
func WithCircuitBreaker(cb *gobreaker.CircuitBreaker) Option {
	return func(o *Options) {
		if cb != nil {
			o.CB = cb
		}
	}
}

// DefaultOptions returns hook callout client defaults.
func DefaultOptions(opts ...Option) Options {
	o := Options{
		Timeout: defaultTimeout,
		Logger:  slog.Default(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.CB == nil {
		o.CB = DefaultCircuitBreaker(o.Logger)
	}
	return o
}

// execute runs fn through the circuit breaker when one is configured. When the
// breaker is open, calls fail fast without reaching the hook service.
func (o Options) execute(fn func() (any, error)) (any, error) {
	if o.CB == nil {
		return fn()
	}
	return o.CB.Execute(fn)
}

// DefaultCircuitBreaker creates a circuit breaker with default settings.
func DefaultCircuitBreaker(logger *slog.Logger) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "hook-callout",
		MaxRequests: defaultCBMaxRequests,
		Interval:    0,
		Timeout:     defaultCBOpenInterval,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= defaultCBFailThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("hook callout circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()))
		},
	})
}
