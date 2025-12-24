// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds OpenTelemetry metric instruments for the MQTT broker.
type Metrics struct {
	meter metric.Meter

	// Counters
	connectionsTotal    metric.Int64Counter
	disconnectionsTotal metric.Int64Counter
	messagesReceived    metric.Int64Counter
	messagesSent        metric.Int64Counter
	bytesReceived       metric.Int64Counter
	bytesSent           metric.Int64Counter
	errorsTotal         metric.Int64Counter

	// UpDownCounters (Gauges)
	connectionsCurrent   metric.Int64UpDownCounter
	subscriptionsActive  metric.Int64UpDownCounter
	retainedMessages     metric.Int64UpDownCounter
	sessionsActive       metric.Int64UpDownCounter

	// Histograms
	messageSize      metric.Int64Histogram
	publishDuration  metric.Float64Histogram
	deliveryDuration metric.Float64Histogram
}

// NewMetrics creates a new Metrics instance with all instruments initialized.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{
		meter: otel.Meter("mqtt-broker"),
	}

	var err error

	// Initialize counters
	m.connectionsTotal, err = m.meter.Int64Counter(
		"mqtt.connections.total",
		metric.WithDescription("Total number of MQTT connections"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connectionsTotal counter: %w", err)
	}

	m.disconnectionsTotal, err = m.meter.Int64Counter(
		"mqtt.disconnections.total",
		metric.WithDescription("Total number of MQTT disconnections"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create disconnectionsTotal counter: %w", err)
	}

	m.messagesReceived, err = m.meter.Int64Counter(
		"mqtt.messages.received.total",
		metric.WithDescription("Total messages received from clients"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messagesReceived counter: %w", err)
	}

	m.messagesSent, err = m.meter.Int64Counter(
		"mqtt.messages.sent.total",
		metric.WithDescription("Total messages sent to clients"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messagesSent counter: %w", err)
	}

	m.bytesReceived, err = m.meter.Int64Counter(
		"mqtt.bytes.received.total",
		metric.WithDescription("Total bytes received"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytesReceived counter: %w", err)
	}

	m.bytesSent, err = m.meter.Int64Counter(
		"mqtt.bytes.sent.total",
		metric.WithDescription("Total bytes sent"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytesSent counter: %w", err)
	}

	m.errorsTotal, err = m.meter.Int64Counter(
		"mqtt.errors.total",
		metric.WithDescription("Total errors by type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create errorsTotal counter: %w", err)
	}

	// Initialize up/down counters (gauges)
	m.connectionsCurrent, err = m.meter.Int64UpDownCounter(
		"mqtt.connections.current",
		metric.WithDescription("Current number of active MQTT connections"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connectionsCurrent gauge: %w", err)
	}

	m.subscriptionsActive, err = m.meter.Int64UpDownCounter(
		"mqtt.subscriptions.active",
		metric.WithDescription("Number of active subscriptions"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriptionsActive gauge: %w", err)
	}

	m.retainedMessages, err = m.meter.Int64UpDownCounter(
		"mqtt.retained.messages",
		metric.WithDescription("Number of retained messages"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retainedMessages gauge: %w", err)
	}

	m.sessionsActive, err = m.meter.Int64UpDownCounter(
		"mqtt.sessions.active",
		metric.WithDescription("Number of active sessions"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create sessionsActive gauge: %w", err)
	}

	// Initialize histograms
	m.messageSize, err = m.meter.Int64Histogram(
		"mqtt.message.size.bytes",
		metric.WithDescription("Message payload size distribution"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create messageSize histogram: %w", err)
	}

	m.publishDuration, err = m.meter.Float64Histogram(
		"mqtt.publish.duration.ms",
		metric.WithDescription("Publish processing duration in milliseconds"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create publishDuration histogram: %w", err)
	}

	m.deliveryDuration, err = m.meter.Float64Histogram(
		"mqtt.delivery.duration.ms",
		metric.WithDescription("Message delivery duration in milliseconds"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create deliveryDuration histogram: %w", err)
	}

	return m, nil
}

// RecordConnection records a new connection.
func (m *Metrics) RecordConnection(protocol, version string) {
	ctx := context.Background()
	m.connectionsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("protocol", protocol),
		attribute.String("version", version),
	))
	m.connectionsCurrent.Add(ctx, 1)
}

// RecordDisconnection records a disconnection.
func (m *Metrics) RecordDisconnection(reason string) {
	ctx := context.Background()
	m.disconnectionsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("reason", reason),
	))
	m.connectionsCurrent.Add(ctx, -1)
}

// RecordMessageReceived records a message received from a client.
func (m *Metrics) RecordMessageReceived(qos byte, sizeBytes int64) {
	ctx := context.Background()
	m.messagesReceived.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("qos", int(qos)),
	))
	m.bytesReceived.Add(ctx, sizeBytes)
	m.messageSize.Record(ctx, sizeBytes)
}

// RecordMessageSent records a message sent to a client.
func (m *Metrics) RecordMessageSent(qos byte, sizeBytes int64) {
	ctx := context.Background()
	m.messagesSent.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("qos", int(qos)),
	))
	m.bytesSent.Add(ctx, sizeBytes)
}

// RecordSubscriptionAdded records a new subscription.
func (m *Metrics) RecordSubscriptionAdded() {
	m.subscriptionsActive.Add(context.Background(), 1)
}

// RecordSubscriptionRemoved records a subscription removal.
func (m *Metrics) RecordSubscriptionRemoved() {
	m.subscriptionsActive.Add(context.Background(), -1)
}

// RecordRetainedSet records a retained message being set.
func (m *Metrics) RecordRetainedSet() {
	m.retainedMessages.Add(context.Background(), 1)
}

// RecordRetainedDeleted records a retained message being deleted.
func (m *Metrics) RecordRetainedDeleted() {
	m.retainedMessages.Add(context.Background(), -1)
}

// RecordError records an error by type.
func (m *Metrics) RecordError(errorType string) {
	m.errorsTotal.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("type", errorType),
	))
}

// RecordPublishDuration records the duration of a publish operation.
func (m *Metrics) RecordPublishDuration(durationMs float64) {
	m.publishDuration.Record(context.Background(), durationMs)
}

// RecordDeliveryDuration records the duration of a message delivery.
func (m *Metrics) RecordDeliveryDuration(durationMs float64) {
	m.deliveryDuration.Record(context.Background(), durationMs)
}
