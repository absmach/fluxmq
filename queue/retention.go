// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

func (m *Manager) offsetByTime(ctx context.Context, queueName string, ts time.Time) (uint64, error) {
	if provider, ok := m.queueStore.(storage.TimeOffsetProvider); ok {
		return provider.OffsetByTime(ctx, queueName, ts)
	}
	return m.queueStore.Head(ctx, queueName)
}

func (m *Manager) offsetBySize(ctx context.Context, queueName string, retentionBytes int64) (uint64, error) {
	if provider, ok := m.queueStore.(storage.SizeOffsetProvider); ok {
		return provider.OffsetBySize(ctx, queueName, retentionBytes)
	}
	return m.queueStore.Head(ctx, queueName)
}

func (m *Manager) computeRetentionOffset(ctx context.Context, config *types.QueueConfig) (uint64, bool) {
	if config == nil {
		return 0, false
	}

	var offset uint64
	hasRetention := false

	if config.Retention.RetentionTime > 0 {
		cutoff := time.Now().Add(-config.Retention.RetentionTime)
		if off, err := m.offsetByTime(ctx, config.Name, cutoff); err == nil {
			if off > offset {
				offset = off
			}
			hasRetention = true
		}
	}

	if config.Retention.RetentionBytes > 0 {
		if off, err := m.offsetBySize(ctx, config.Name, config.Retention.RetentionBytes); err == nil {
			if off > offset {
				offset = off
			}
			hasRetention = true
		}
	}

	if config.Retention.RetentionMessages > 0 {
		head, err := m.queueStore.Head(ctx, config.Name)
		if err == nil {
			tail, err := m.queueStore.Tail(ctx, config.Name)
			if err == nil {
				if tail > head+uint64(config.Retention.RetentionMessages) {
					msgOffset := tail - uint64(config.Retention.RetentionMessages)
					if msgOffset > offset {
						offset = msgOffset
					}
				} else if head > offset {
					offset = head
				}
				hasRetention = true
			}
		}
	}

	return offset, hasRetention
}
