// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/absmach/mqtt/broker/events"
	"github.com/absmach/mqtt/config"
	"github.com/sony/gobreaker"
)

// GenericNotifier implements webhook notifications with worker pool and circuit breaker.
type GenericNotifier struct {
	cfg            config.WebhookConfig
	brokerID       string
	endpoints      []endpointConfig
	eventQueue     chan eventJob
	breakers       map[string]*gobreaker.CircuitBreaker
	sender         Sender
	logger         *slog.Logger
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	includePayload bool
}

type endpointConfig struct {
	name         string
	url          string
	eventFilters map[string]bool // event type filters
	topicFilters []string        // topic pattern filters
	headers      map[string]string
	timeout      time.Duration
	retryConfig  config.RetryConfig
}

type eventJob struct {
	event    events.Event
	endpoint endpointConfig
	attempt  int
}

// NewNotifier creates a new generic webhook notifier.
func NewNotifier(cfg config.WebhookConfig, brokerID string, sender Sender, logger *slog.Logger) (*GenericNotifier, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if sender == nil {
		return nil, fmt.Errorf("sender cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Parse endpoint configurations
	endpoints := make([]endpointConfig, 0, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		// Build event filter map for fast lookup
		eventFilters := make(map[string]bool)
		if len(ep.Events) > 0 {
			for _, eventType := range ep.Events {
				eventFilters[eventType] = true
			}
		}

		// Use endpoint-specific timeout or default
		timeout := cfg.Defaults.Timeout
		if ep.Timeout > 0 {
			timeout = ep.Timeout
		}

		// Use endpoint-specific retry config or default
		retryConfig := cfg.Defaults.Retry
		if ep.Retry != nil {
			retryConfig = *ep.Retry
		}

		endpoints = append(endpoints, endpointConfig{
			name:         ep.Name,
			url:          ep.URL,
			eventFilters: eventFilters,
			topicFilters: ep.TopicFilters,
			headers:      ep.Headers,
			timeout:      timeout,
			retryConfig:  retryConfig,
		})
	}

	// Create circuit breakers per endpoint
	breakers := make(map[string]*gobreaker.CircuitBreaker)
	for _, ep := range endpoints {
		breakers[ep.name] = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        ep.name,
			MaxRequests: 1,
			Interval:    0,
			Timeout:     cfg.Defaults.CircuitBreaker.ResetTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= uint32(cfg.Defaults.CircuitBreaker.FailureThreshold)
			},
			OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
				logger.Warn("webhook circuit breaker state changed",
					slog.String("endpoint", name),
					slog.String("from", from.String()),
					slog.String("to", to.String()))
			},
		})
	}

	n := &GenericNotifier{
		cfg:            cfg,
		brokerID:       brokerID,
		endpoints:      endpoints,
		eventQueue:     make(chan eventJob, cfg.QueueSize),
		breakers:       breakers,
		sender:         sender,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		includePayload: cfg.IncludePayload,
	}

	// Start worker pool
	for i := 0; i < cfg.Workers; i++ {
		n.wg.Add(1)
		go n.worker(i)
	}

	logger.Info("webhook notifier started",
		slog.Int("workers", cfg.Workers),
		slog.Int("queue_size", cfg.QueueSize),
		slog.Int("endpoints", len(endpoints)))

	return n, nil
}

// Notify sends an event to all matching endpoints asynchronously.
func (n *GenericNotifier) Notify(ctx context.Context, event interface{}) error {
	// Cast to events.Event
	ev, ok := event.(events.Event)
	if !ok {
		return fmt.Errorf("event must implement events.Event interface")
	}

	// Filter endpoints and queue jobs
	for _, endpoint := range n.endpoints {
		if !n.shouldNotify(endpoint, ev) {
			continue
		}

		job := eventJob{
			event:    ev,
			endpoint: endpoint,
			attempt:  0,
		}

		select {
		case n.eventQueue <- job:
			// Successfully queued
		default:
			// Queue full, apply drop policy
			if n.cfg.DropPolicy == "oldest" {
				// Drop oldest, add newest
				select {
				case <-n.eventQueue: // drop oldest
				default:
				}
				select {
				case n.eventQueue <- job: // try to add newest
				default:
					n.logger.Error("webhook queue full, event dropped",
						slog.String("event_type", ev.Type()),
						slog.String("endpoint", endpoint.name))
				}
			} else {
				// Drop newest (this one)
				n.logger.Error("webhook queue full, event dropped",
					slog.String("event_type", ev.Type()),
					slog.String("endpoint", endpoint.name))
			}
		}
	}

	return nil
}

// shouldNotify checks if an endpoint should be notified for this event.
func (n *GenericNotifier) shouldNotify(endpoint endpointConfig, event events.Event) bool {
	// Check event type filter
	if len(endpoint.eventFilters) > 0 {
		if !endpoint.eventFilters[event.Type()] {
			return false
		}
	}

	// Check topic filter (only for message events)
	if event.Topic() != "" && len(endpoint.topicFilters) > 0 {
		matched := false
		for _, filter := range endpoint.topicFilters {
			if topicMatches(filter, event.Topic()) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// topicMatches checks if a topic matches a filter pattern (supports MQTT wildcards).
func topicMatches(filter, topic string) bool {
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	fi := 0
	ti := 0

	for fi < len(filterParts) {
		if filterParts[fi] == "#" {
			return true // Multi-level wildcard matches everything
		}

		if ti >= len(topicParts) {
			return false
		}

		if filterParts[fi] != "+" && filterParts[fi] != topicParts[ti] {
			return false
		}

		fi++
		ti++
	}

	return ti == len(topicParts)
}

// worker processes events from the queue.
func (n *GenericNotifier) worker(id int) {
	defer n.wg.Done()

	for {
		select {
		case <-n.ctx.Done():
			return
		case job := <-n.eventQueue:
			n.processJob(job)
		}
	}
}

// processJob sends a webhook with retry logic.
func (n *GenericNotifier) processJob(job eventJob) {
	breaker := n.breakers[job.endpoint.name]

	// Wrap the send call with circuit breaker
	_, err := breaker.Execute(func() (interface{}, error) {
		return nil, n.sendWebhook(job)
	})

	if err != nil {
		// Retry logic
		if job.attempt < job.endpoint.retryConfig.MaxAttempts-1 {
			job.attempt++
			delay := n.calculateRetryDelay(job.attempt, job.endpoint.retryConfig)

			n.logger.Debug("webhook delivery failed, retrying",
				slog.String("endpoint", job.endpoint.name),
				slog.String("event_type", job.event.Type()),
				slog.Int("attempt", job.attempt),
				slog.Duration("retry_after", delay),
				slog.String("error", err.Error()))

			// Schedule retry
			time.AfterFunc(delay, func() {
				select {
				case n.eventQueue <- job:
				default:
					n.logger.Error("failed to requeue event for retry",
						slog.String("endpoint", job.endpoint.name),
						slog.String("event_type", job.event.Type()))
				}
			})
		} else {
			// Max retries exhausted
			n.logger.Error("webhook delivery failed after max retries",
				slog.String("endpoint", job.endpoint.name),
				slog.String("event_type", job.event.Type()),
				slog.Int("attempts", job.attempt+1),
				slog.String("error", err.Error()))
		}
	}
}

// sendWebhook marshals the event and delegates to the protocol-specific sender.
func (n *GenericNotifier) sendWebhook(job eventJob) error {
	// Wrap event in envelope
	envelope := job.event.Wrap(n.brokerID)

	// Marshal to JSON
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create context with endpoint-specific timeout
	ctx, cancel := context.WithTimeout(context.Background(), job.endpoint.timeout)
	defer cancel()

	// Delegate to protocol-specific sender
	if err := n.sender.Send(ctx, job.endpoint.url, job.endpoint.headers, payload, job.endpoint.timeout); err != nil {
		return err
	}

	n.logger.Debug("webhook delivered successfully",
		slog.String("endpoint", job.endpoint.name),
		slog.String("event_type", job.event.Type()))

	return nil
}

// calculateRetryDelay calculates exponential backoff delay.
func (n *GenericNotifier) calculateRetryDelay(attempt int, cfg config.RetryConfig) time.Duration {
	delay := float64(cfg.InitialInterval) * pow(cfg.Multiplier, float64(attempt))
	if delay > float64(cfg.MaxInterval) {
		delay = float64(cfg.MaxInterval)
	}
	return time.Duration(delay)
}

// pow is a simple integer exponentiation (to avoid importing math).
func pow(base float64, exp float64) float64 {
	result := 1.0
	for i := 0; i < int(exp); i++ {
		result *= base
	}
	return result
}

// Close gracefully shuts down the notifier.
func (n *GenericNotifier) Close() error {
	n.logger.Info("shutting down webhook notifier")

	// Stop accepting new events
	n.cancel()

	// Wait for queue to drain with timeout
	done := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		n.logger.Info("webhook notifier stopped gracefully")
	case <-time.After(n.cfg.ShutdownTimeout):
		n.logger.Warn("webhook notifier shutdown timeout, some events may be lost",
			slog.Int("queue_depth", len(n.eventQueue)))
	}

	return nil
}
