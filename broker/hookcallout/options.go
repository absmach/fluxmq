// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hookcallout

import (
	"log/slog"
	"time"
)

const defaultTimeout = 500 * time.Millisecond

// Options holds hook callout options.
type Options struct {
	Timeout time.Duration
	Logger  *slog.Logger
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

// DefaultOptions returns hook callout client defaults.
func DefaultOptions(opts ...Option) Options {
	o := Options{
		Timeout: defaultTimeout,
		Logger:  slog.Default(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
