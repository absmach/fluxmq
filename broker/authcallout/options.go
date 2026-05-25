// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/sony/gobreaker"
)

const (
	defaultTimeout         = 2 * time.Second
	defaultCBMaxRequests   = 3
	defaultCBOpenInterval  = 10 * time.Second
	defaultCBFailThreshold = 5
	defaultRetries         = 1
	defaultRetryBackoff    = 100 * time.Millisecond
	defaultRetryMaxBackoff = 1 * time.Second
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
	Timeout         time.Duration
	Logger          *slog.Logger
	CB              *gobreaker.CircuitBreaker
	Protocol        Protocol
	Retries         int           // additional attempts after the first; 0 disables retry
	RetryBackoff    time.Duration // base delay for exponential backoff
	RetryMaxBackoff time.Duration // cap for backoff delay
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

// WithRetries sets the number of additional attempts after the initial call.
// Retries trigger on transient transport errors only; the circuit breaker
// still short-circuits attempts when open.
func WithRetries(n int) Option {
	return func(o *Options) {
		if n >= 0 {
			o.Retries = n
		}
	}
}

// WithRetryBackoff sets the base delay used for exponential backoff between
// retries. The actual delay is base * 2^attempt with full jitter, capped by
// the value provided to WithRetryMaxBackoff.
func WithRetryBackoff(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.RetryBackoff = d
		}
	}
}

// WithRetryMaxBackoff caps the per-attempt backoff delay.
func WithRetryMaxBackoff(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.RetryMaxBackoff = d
		}
	}
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions(opts ...Option) Options {
	o := Options{
		Timeout:         defaultTimeout,
		Logger:          slog.Default(),
		Retries:         defaultRetries,
		RetryBackoff:    defaultRetryBackoff,
		RetryMaxBackoff: defaultRetryMaxBackoff,
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.CB == nil {
		o.CB = DefaultCircuitBreaker(o.Logger)
	}
	return o
}

// retryWithBackoff invokes fn up to Retries+1 times. The shouldRetry callback
// decides whether to retry on a returned error; non-retriable errors return
// immediately. Sleeps respect ctx cancellation.
func (o Options) retryWithBackoff(ctx context.Context, fn func() (any, error), shouldRetry func(error) bool) (any, error) {
	var lastErr error
	for attempt := 0; attempt <= o.Retries; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !shouldRetry(err) || attempt == o.Retries {
			return result, err
		}

		delay := o.backoffDelay(attempt)
		if delay <= 0 {
			continue
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, errors.Join(lastErr, ctx.Err())
		}
	}
	return nil, lastErr
}

func (o Options) backoffDelay(attempt int) time.Duration {
	base := o.RetryBackoff
	if base <= 0 {
		return 0
	}
	d := base << attempt
	if o.RetryMaxBackoff > 0 && d > o.RetryMaxBackoff {
		d = o.RetryMaxBackoff
	}
	// Full jitter: uniform [0, d).
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// retriableError reports whether the error is worth a retry attempt.
// Circuit-breaker rejections and explicit caller cancellation are surfaced
// immediately. Per-attempt deadline exceeded is retriable because each
// attempt receives its own timeout.
func retriableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
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
			logger.Debug("auth callout circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()))
		},
	})
}
