// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"log/slog"
	"time"

	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/sony/gobreaker"
)

const (
	defaultTimeout         = 2 * time.Second
	defaultCBMaxRequests   = 3
	defaultCBOpenInterval  = 10 * time.Second
	defaultCBFailThreshold = 5
)

// Protocol identifies which messaging protocol the client connected with.
type Protocol = authv1.Protocol

const (
	ProtocolUnspecified = authv1.Protocol_Unspecified
	ProtocolMQTT        = authv1.Protocol_MQTT
	ProtocolAMQP10      = authv1.Protocol_AMQP_1_0
	ProtocolAMQP091     = authv1.Protocol_AMQP_0_9_1
)

// Options holds configuration shared by all callout transports.
type Options struct {
	Timeout  time.Duration
	Logger   *slog.Logger
	CB       *gobreaker.CircuitBreaker
	Protocol Protocol
}

// Option configures a callout client.
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

// WithProtocol sets the protocol identifier sent in authentication requests.
func WithProtocol(p Protocol) Option {
	return func(o *Options) {
		o.Protocol = p
	}
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions(opts ...Option) Options {
	o := Options{
		Timeout: defaultTimeout,
		Logger:  slog.Default(),
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.CB == nil {
		o.CB = DefaultCircuitBreaker(o.Logger)
	}
	return o
}

// DefaultCircuitBreaker creates a circuit breaker with default settings.
func DefaultCircuitBreaker(logger *slog.Logger) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "auth-callout",
		MaxRequests: defaultCBMaxRequests,
		Interval:    0,
		Timeout:     defaultCBOpenInterval,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= defaultCBFailThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("auth callout circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()))
		},
	})
}
