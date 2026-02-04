// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds OpenTelemetry metric instruments for the AMQP broker.
type Metrics struct {
	meter metric.Meter

	connectionsTotal    metric.Int64Counter
	disconnectionsTotal metric.Int64Counter
	messagesReceived    metric.Int64Counter
	messagesSent        metric.Int64Counter
	bytesReceived       metric.Int64Counter
	bytesSent           metric.Int64Counter
	errorsTotal         metric.Int64Counter

	connectionsCurrent  metric.Int64UpDownCounter
	sessionsCurrent     metric.Int64UpDownCounter
	linksCurrent        metric.Int64UpDownCounter
	subscriptionsActive metric.Int64UpDownCounter

	messageSize metric.Int64Histogram
}

// NewMetrics creates a new AMQP Metrics instance with all instruments initialized.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{
		meter: otel.Meter("amqp-broker"),
	}

	var err error

	m.connectionsTotal, err = m.meter.Int64Counter(
		"amqp.connections.total",
		metric.WithDescription("Total number of AMQP connections"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp connectionsTotal counter: %w", err)
	}

	m.disconnectionsTotal, err = m.meter.Int64Counter(
		"amqp.disconnections.total",
		metric.WithDescription("Total number of AMQP disconnections"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp disconnectionsTotal counter: %w", err)
	}

	m.messagesReceived, err = m.meter.Int64Counter(
		"amqp.messages.received.total",
		metric.WithDescription("Total AMQP messages received from clients"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp messagesReceived counter: %w", err)
	}

	m.messagesSent, err = m.meter.Int64Counter(
		"amqp.messages.sent.total",
		metric.WithDescription("Total AMQP messages sent to clients"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp messagesSent counter: %w", err)
	}

	m.bytesReceived, err = m.meter.Int64Counter(
		"amqp.bytes.received.total",
		metric.WithDescription("Total AMQP bytes received"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp bytesReceived counter: %w", err)
	}

	m.bytesSent, err = m.meter.Int64Counter(
		"amqp.bytes.sent.total",
		metric.WithDescription("Total AMQP bytes sent"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp bytesSent counter: %w", err)
	}

	m.errorsTotal, err = m.meter.Int64Counter(
		"amqp.errors.total",
		metric.WithDescription("Total AMQP errors by type"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp errorsTotal counter: %w", err)
	}

	m.connectionsCurrent, err = m.meter.Int64UpDownCounter(
		"amqp.connections.current",
		metric.WithDescription("Current number of active AMQP connections"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp connectionsCurrent gauge: %w", err)
	}

	m.sessionsCurrent, err = m.meter.Int64UpDownCounter(
		"amqp.sessions.current",
		metric.WithDescription("Current number of active AMQP sessions"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp sessionsCurrent gauge: %w", err)
	}

	m.linksCurrent, err = m.meter.Int64UpDownCounter(
		"amqp.links.current",
		metric.WithDescription("Current number of active AMQP links"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp linksCurrent gauge: %w", err)
	}

	m.subscriptionsActive, err = m.meter.Int64UpDownCounter(
		"amqp.subscriptions.active",
		metric.WithDescription("Number of active AMQP subscriptions"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp subscriptionsActive gauge: %w", err)
	}

	m.messageSize, err = m.meter.Int64Histogram(
		"amqp.message.size.bytes",
		metric.WithDescription("AMQP message payload size distribution"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create amqp messageSize histogram: %w", err)
	}

	return m, nil
}

func (m *Metrics) RecordConnection() {
	ctx := context.Background()
	m.connectionsTotal.Add(ctx, 1)
	m.connectionsCurrent.Add(ctx, 1)
}

func (m *Metrics) RecordDisconnection() {
	ctx := context.Background()
	m.disconnectionsTotal.Add(ctx, 1)
	m.connectionsCurrent.Add(ctx, -1)
}

func (m *Metrics) RecordMessageReceived(sizeBytes int64) {
	ctx := context.Background()
	m.messagesReceived.Add(ctx, 1)
	m.bytesReceived.Add(ctx, sizeBytes)
	m.messageSize.Record(ctx, sizeBytes)
}

func (m *Metrics) RecordMessageSent(sizeBytes int64) {
	ctx := context.Background()
	m.messagesSent.Add(ctx, 1)
	m.bytesSent.Add(ctx, sizeBytes)
}

func (m *Metrics) RecordSessionOpened() {
	m.sessionsCurrent.Add(context.Background(), 1)
}

func (m *Metrics) RecordSessionClosed() {
	m.sessionsCurrent.Add(context.Background(), -1)
}

func (m *Metrics) RecordLinkAttached() {
	m.linksCurrent.Add(context.Background(), 1)
}

func (m *Metrics) RecordLinkDetached() {
	m.linksCurrent.Add(context.Background(), -1)
}

func (m *Metrics) RecordSubscriptionAdded() {
	m.subscriptionsActive.Add(context.Background(), 1)
}

func (m *Metrics) RecordSubscriptionRemoved() {
	m.subscriptionsActive.Add(context.Background(), -1)
}

func (m *Metrics) RecordError(errorType string) {
	m.errorsTotal.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("type", errorType),
	))
}
