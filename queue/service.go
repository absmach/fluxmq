// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
)

// Compile-time interface assertions — Manager implements each narrow contract
// that consumers depend on individually.
var (
	_ broker.QueueManager       = (*Manager)(nil)
	_ broker.StreamQueueManager = (*Manager)(nil)
	_ cluster.QueueHandler      = (*Manager)(nil)
	_ StreamCommitter           = (*Manager)(nil)
	_ MetricsProvider           = (*Manager)(nil)
)

// StreamCommitter exposes explicit commit control for stream consumer groups.
type StreamCommitter interface {
	CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error
}

// MetricsProvider exposes read-only queue delivery metrics.
type MetricsProvider interface {
	GetMetrics() consumer.Metrics
	GetLag(ctx context.Context, queueName, groupID string) (uint64, error)
}
